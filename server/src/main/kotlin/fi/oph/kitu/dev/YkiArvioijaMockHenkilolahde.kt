package fi.oph.kitu.dev

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.MockHenkilolahde
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.yki.arvioijat.YkiArvioijaArviointioikeus
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/**
 * Offline-kehityksessa kannassa olevat arvioijat eivat loydy ONR-fixtureista, jolloin heidan
 * tietojaan ei voi muokata. Rivit luetaan siksi kaynnistyksessa muistiin
 * mock-ONR:n henkiloiksi.
 */
@Component
@Profile("local-opintopolku")
class YkiArvioijaMockHenkilolahde(
    private val repository: YkiArvioijaRepository,
) : MockHenkilolahde,
    ApplicationRunner {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var arvioijaHenkilot: Map<Oid, OppijanumerorekisteriHenkilo> = emptyMap()

    override fun henkilot(): Map<Oid, OppijanumerorekisteriHenkilo> = arvioijaHenkilot

    override fun run(args: ApplicationArguments) {
        arvioijaHenkilot =
            repository
                .allArviontioikeudet()
                .associate { it.arvioijanOppijanumero to henkiloksi(it) }
        logger.info("Ladattiin {} YKI-arvioijaa mock-oppijanumerorekisteriin", arvioijaHenkilot.size)
    }

    private fun henkiloksi(arvioija: YkiArvioijaArviointioikeus): OppijanumerorekisteriHenkilo {
        val oid = arvioija.arvioijanOppijanumero.toString()
        return OppijanumerorekisteriHenkilo(
            oidHenkilo = oid,
            hetu = arvioija.henkilotunnus,
            kaikkiHetut = listOfNotNull(arvioija.henkilotunnus),
            passivoitu = false,
            etunimet = arvioija.etunimet,
            kutsumanimi = arvioija.etunimet.substringBefore(" "),
            sukunimi = arvioija.sukunimi,
            aidinkieli = null,
            asiointiKieli = null,
            kansalaisuus = null,
            kasittelijaOid = null,
            syntymaaika = null,
            sukupuoli = null,
            kotikunta = null,
            oppijanumero = oid,
            turvakielto = false,
            eiSuomalaistaHetua = arvioija.henkilotunnus == null,
            yksiloity = true,
            yksiloityVTJ = true,
            yksilointiYritetty = true,
            duplicate = false,
            created = null,
            modified = null,
            vtjsynced = null,
            yhteystiedotRyhma = listOf(yhteystiedot(arvioija)),
            yksilointivirheet = emptyList(),
            passinumerot = emptyList(),
        )
    }

    private fun yhteystiedot(arvioija: YkiArvioijaArviointioikeus) =
        OppijanumerorekisteriHenkilo.Yhteystietoryhma(
            id = null,
            ryhmaKuvaus = "yhteystietotyyppi4",
            ryhmaAlkuperaTieto = "alkupera1",
            readOnly = true,
            yhteystieto =
                listOfNotNull(
                    yhteystieto("YHTEYSTIETO_SAHKOPOSTI", arvioija.sahkopostiosoite),
                    yhteystieto("YHTEYSTIETO_MATKAPUHELINNUMERO", arvioija.puhelinnumero),
                    yhteystieto("YHTEYSTIETO_KATUOSOITE", arvioija.katuosoite),
                    yhteystieto("YHTEYSTIETO_POSTINUMERO", arvioija.postinumero),
                    yhteystieto("YHTEYSTIETO_KAUPUNKI", arvioija.postitoimipaikka),
                ),
        )

    private fun yhteystieto(
        tyyppi: String,
        arvo: String?,
    ) = arvo
        ?.takeIf { it.isNotBlank() }
        ?.let { OppijanumerorekisteriHenkilo.Yhteystietoryhma.Yhteystieto(tyyppi, it) }
}
