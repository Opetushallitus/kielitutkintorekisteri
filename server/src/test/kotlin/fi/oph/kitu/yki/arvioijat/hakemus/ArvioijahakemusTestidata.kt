package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.AtaruException
import fi.oph.kitu.ataru.AtaruLasku
import fi.oph.kitu.ataru.HakemusOtsake
import fi.oph.kitu.ataru.OptionAnswer
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.ataru.SiirtoJasennysvirhe
import fi.oph.kitu.ataru.StatesAndFilters
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

object ArvioijahakemusTestidata {
    /** Mock-ONR:n henkilo, jolla on osoite ja sahkoposti. */
    const val PETRO_OID = "1.2.246.562.24.59267607404"

    /** Mock-ONR tuntee taman henkilon, mutta hanella ei ole oppijanumeroa. */
    const val YKSILOIMATON_OID = "1.2.246.562.24.10691606777"

    /** Mock-ONR:n henkilo ilman yhteystietoja. */
    const val OSOITTEETON_OID = "1.2.246.562.24.20281155246"

    /** Mock-ONR ei tunne tata henkiloa. */
    const val TUNTEMATON_OID = "1.2.246.562.24.00000000001"

    val HETKI: Instant = Instant.parse("2026-06-01T09:00:00Z")
    val MYOHEMMIN: Instant = Instant.parse("2026-06-02T09:00:00Z")
    val TANAAN: LocalDate = LocalDate.of(2026, 6, 1)

    val lomake =
        ArvioijahakemusLomake(
            lomakkeenAvain = "lomake",
            kielikysymys = "kielet",
            kielet =
                mapOf(
                    "0" to
                        ArvioijahakemusLomake.Kielivaihtoehto(
                            Tutkintokieli.FIN,
                            "tasot",
                            mapOf("0" to setOf(Tutkintotaso.PT, Tutkintotaso.KT), "1" to setOf(Tutkintotaso.YT)),
                        ),
                ),
        )

    fun siirtoHakemus(
        oid: String,
        personOid: String,
        keyValues: String = """{"kielet_0":"0","tasot_0":"0"}""",
    ) = SiirtoHakemus(
        hakemusOid = oid,
        personOid = personOid,
        keyValues =
            defaultObjectMapper
                .readTree(keyValues)
                .properties()
                .associate { it.key to it.value },
    )

    fun tarkistettava(
        hakemusOid: String,
        henkiloOid: String = PETRO_OID,
        hakemus: Arvioijahakemus =
            Arvioijahakemus(
                arviointioikeudet =
                    listOf(
                        TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT, Tutkintotaso.KT)),
                    ),
                sahkopostiosoite = null,
                katuosoite = null,
                postinumero = null,
                postitoimipaikka = null,
            ),
    ) = TarkistettavaHakemus(
        hakemusOid = hakemusOid,
        henkiloOid = henkiloOid,
        arvioijaOid = henkiloOid,
        sukunimi = "Kivinen-Testi",
        etunimet = "Petro Testi",
        hakemus = hakemus,
        ensimmainenHaku = HETKI.atOffset(ZoneOffset.UTC),
        viimeisinHaku = HETKI.atOffset(ZoneOffset.UTC),
    )

    fun ArvioijahakemusRepository.kirjaaKasitellyksi(
        hakemusOid: String,
        henkiloOid: String = PETRO_OID,
    ) {
        varaa(hakemusOid, henkiloOid, TANAAN, HETKI.atOffset(ZoneOffset.UTC))
        merkitseKasitellyksi(hakemusOid, henkiloOid, null, TANAAN, HETKI.atOffset(ZoneOffset.UTC))
    }

    fun oid(s: String): Oid = Oid.parse(s).getOrThrow()
}

class FakeAtaruClient : AtaruClient {
    var hakemukset: List<SiirtoHakemus> = emptyList()
    var jasentymattomat: List<SiirtoJasennysvirhe> = emptyList()
    var maksut: Map<String, List<AtaruLasku>> = emptyMap()
    var maksutEpaonnistuvat = false
    val siirtokutsut = mutableListOf<List<String>>()
    val listauskutsut = mutableListOf<String>()

    fun tyhjenna() {
        hakemukset = emptyList()
        jasentymattomat = emptyList()
        maksut = emptyMap()
        maksutEpaonnistuvat = false
        siirtokutsut.clear()
        listauskutsut.clear()
    }

    override fun haeMaksut(hakemusOid: String): Either<AtaruException, List<AtaruLasku>> =
        if (maksutEpaonnistuvat) {
            AtaruException.NullResponse("api/maksut/list/$hakemusOid").left()
        } else {
            maksut[hakemusOid].orEmpty().right()
        }

    override fun haeHakemusavaimet(
        formKey: String,
        ehdot: List<OptionAnswer>,
        tilat: StatesAndFilters,
    ): Either<AtaruException, List<HakemusOtsake>> {
        listauskutsut += formKey
        return (hakemukset.map { it.hakemusOid } + jasentymattomat.mapNotNull { it.hakemusOid })
            .map { HakemusOtsake(it) }
            .right()
    }

    override fun haeHakemukset(
        avaimet: List<String>,
    ): Either<AtaruException, List<Either<SiirtoJasennysvirhe, SiirtoHakemus>>> {
        siirtokutsut += avaimet
        return (
            hakemukset.filter { it.hakemusOid in avaimet }.map { Either.Right(it) } +
                jasentymattomat.filter { it.hakemusOid == null || it.hakemusOid in avaimet }.map { Either.Left(it) }
        ).right()
    }
}
