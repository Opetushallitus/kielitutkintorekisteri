package fi.oph.kitu.yki.arvioijat.solki

import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import java.time.LocalDate

enum class Lahetystulos {
    LAHETETTY,
    VIRHE,

    /** Automaattilahetys on kytketty pois: rivi jaa jonoon ja lahtee kun kytkin avataan. */
    EI_KAYTOSSA,
}

interface SolkiArvioijaService {
    /**
     * Yksi synkroninen yritys tallennuksen jalkeen, jotta virkailija nakee tuloksen heti.
     * Integraatiokytkimen takana: pois paalta rivi jaa jonoon.
     */
    fun lahetaArvioija(arvioija: YkiArvioijaEntity): Lahetystulos

    /**
     * Virkailijan itse kaynnistama lahetys tietosivun painikkeesta. Ei katso integraatiokytkinta:
     * yksittainen lahetys on nimenomaan se tapa, jolla integraatiota paastaan kokeilemaan ennen
     * kuin automaattinen liikenne avataan.
     */
    fun lahetaArvioijaKasin(arvioija: YkiArvioijaEntity): Lahetystulos

    /**
     * @param maxYritykset null = kaikki lahettamattomat, myos pitkaan epaonnistuneet.
     * @return onnistuneiden lahetysten maara.
     */
    fun lahetaLahettamattomat(maxYritykset: Int? = null): Int
}

// open, koska @WithSpan vaatii CGLIB-proxyn: ilman @Service-annotaatiota Kotlinin allopen ei
// enaa avaa luokkaa.
open class SolkiArvioijaServiceImpl(
    private val repository: YkiArvioijaRepository,
    private val client: SolkiArvioijaClient,
    private val timeService: TimeService,
    private val oppijanumeroService: OppijanumeroService,
    private val asetukset: ArvioijarekisteriAsetukset,
) : SolkiArvioijaService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    override fun lahetaArvioija(arvioija: YkiArvioijaEntity): Lahetystulos =
        if (asetukset.integraatioKaytossa) laheta(arvioija) else Lahetystulos.EI_KAYTOSSA

    @WithSpan
    override fun lahetaArvioijaKasin(arvioija: YkiArvioijaEntity): Lahetystulos = laheta(arvioija)

    @WithSpan
    override fun lahetaLahettamattomat(maxYritykset: Int?): Int {
        if (!asetukset.integraatioKaytossa) return 0

        val lahetettavat = repository.findLahetettavat(maxYritykset)
        val onnistuneet = lahetettavat.count { laheta(it) == Lahetystulos.LAHETETTY }

        Span
            .current()
            .setAttribute("arvioijat.yritetty", lahetettavat.size.toLong())
            .setAttribute("arvioijat.lahetetty", onnistuneet.toLong())

        return onnistuneet
    }

    /**
     * Yksikin odottamaton poikkeus ei saa keskeyttaa eraa: muuten yksi rikkinainen rivi estaisi
     * koko jonon lahetyksen eivatka muut rivit saisi edes virhemerkintaa.
     */
    private fun laheta(arvioija: YkiArvioijaEntity): Lahetystulos {
        val id = arvioija.id?.toInt() ?: return Lahetystulos.VIRHE

        return runCatching {
            oppijanumeroService
                .getHenkiloByMasterOid(arvioija.arvioijaOid)
                .fold(
                    ifLeft = { virhe ->
                        repository.merkitseLahetysvirhe(id, syntymaajanHakuvirhe(arvioija, virhe))
                        Lahetystulos.VIRHE
                    },
                    ifRight = { henkilo -> lahetaSolkiin(id, arvioija, henkilo.syntymaaika) },
                )
        }.getOrElse { e ->
            logger.warn("Arvioijan {} lahetys epaonnistui odottamattomasti", arvioija.arvioijaOid, e)
            repository.merkitseLahetysvirhe(id, "Unexpected failure: ${e.javaClass.simpleName}")
            Lahetystulos.VIRHE
        }
    }

    private fun lahetaSolkiin(
        id: Int,
        arvioija: YkiArvioijaEntity,
        syntymaaika: LocalDate?,
    ): Lahetystulos =
        client
            .laheta(SolkiArvioijaRequest.of(arvioija, timeService.today(), syntymaaika))
            .fold(
                ifLeft = { virhe ->
                    repository.merkitseLahetysvirhe(id, virhe.debugString())
                    Lahetystulos.VIRHE
                },
                ifRight = {
                    // Versioehto: jos rivia on muokattu lahetyksen aikana, se jaa jonoon.
                    repository.merkitseLahetetyksi(id, arvioija.muokattu)
                    Lahetystulos.LAHETETTY
                },
            )

    /**
     * ONR-katkoa ei saa tulkita puuttuvaksi syntymaajaksi: [SolkiArvioijaRequest.of] jattaisi
     * kentan pois, rivi merkittaisiin lahetetyksi eika mikaan lahettaisi sita uudelleen ennen
     * seuraavaa muokkausta. Rivi jatetaan siksi jonoon.
     *
     * Virheesta otetaan vain tyyppi ja tilakoodi. ONR:n vastausrunko on henkilotietoa, ja teksti
     * paatyy solki_lahetysvirhe-sarakkeeseen josta se renderoidaan tietosivulle.
     */
    private fun syntymaajanHakuvirhe(
        arvioija: YkiArvioijaEntity,
        virhe: OppijanumeroException,
    ): String =
        listOfNotNull(
            "Syntymaajan haku oppijanumerorekisterista epaonnistui",
            "oppijanumero: ${arvioija.arvioijaOid}",
            "cause: ${virhe.javaClass.simpleName}",
            (virhe as? OppijanumeroException.HasResponse)?.let { "response status: ${it.response.statusCode}" },
        ).joinToString("; ")
}
