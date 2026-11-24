package fi.oph.kitu.tietosisalto

import arrow.core.nonEmptyListOf
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.ilmoittautumisjarjestelma.YkiArvioinninTila
import fi.oph.kitu.ilmoittautumisjarjestelma.YkiArvioinninTilaRequest
import fi.oph.kitu.ilmoittautumisjarjestelma.YkiSuorituksenTunniste
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koski.KoskiVktRequestMapper
import fi.oph.kitu.koski.KoskiYkiRequestMapper
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.VktSuoritus
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaPoistoRequest
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaRequest
import java.time.LocalDate
import java.time.OffsetDateTime

object Esimerkit {
    private const val ORGANISAATIO = "1.2.246.562.10.12345678910"
    private val oppija = Oid.parse("1.2.246.562.24.12345678910").getOrThrow()
    private val opiskeluoikeus = Oid.parse("1.2.246.562.15.12345678910").getOrThrow()

    fun sanoma(virta: Tietovirta): Any =
        when (virta) {
            Tietovirrat.koskiYki -> koskiYki()
            Tietovirrat.koskiVkt -> koskiVkt()
            Tietovirrat.solkiArvioija -> solkiArvioija()
            Tietovirrat.solkiArvioijanPoisto -> solkiArvioijanPoisto()
            Tietovirrat.kiosArviointitila -> kiosArviointitila()
            else -> error("Tietovirralle ${virta.otsikko} ei ole esimerkkisanomaa")
        }

    private fun koskiYki() =
        KoskiYkiRequestMapper()
            .apply { ykiOrganisaatioOid = ORGANISAATIO }
            .ykiSuoritusToKoskiRequest(
                generateRandomYkiSuoritusEntity().copy(
                    suorittajanOID = oppija,
                    arviointitila = Arviointitila.ARVIOITU,
                    tutkintopaiva = LocalDate.of(2025, 1, 1),
                    arviointipaiva = LocalDate.of(2025, 1, 3),
                    tutkintotaso = Tutkintotaso.PT,
                    tutkintokieli = Tutkintokieli.ENG,
                    jarjestajanTunnusOid = Oid.parse(ORGANISAATIO).getOrThrow(),
                    tekstinYmmartaminen = 2,
                    kirjoittaminen = 2,
                    puheenYmmartaminen = 2,
                    puhuminen = 2,
                    rakenteetJaSanasto = null,
                    yleisarvosana = 2,
                    koskiOpiskeluoikeus = opiskeluoikeus,
                    lahdejarjestelmanTunnus = "yki.123456",
                ),
            ).getOrThrow()

    private fun koskiVkt() =
        KoskiVktRequestMapper()
            .apply { vktOrganisaatioOid = ORGANISAATIO }
            .vktSuoritusToKoskiRequest(
                Henkilosuoritus(
                    henkilo = Henkilo(oid = oppija, etunimet = "Keijo", sukunimi = "Keijunen"),
                    suoritus =
                        VktSuoritus(
                            taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                            kieli = Koodisto.Tutkintokieli.FIN,
                            suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.27639300000").getOrThrow(),
                            suorituspaikkakunta = "091",
                            osat =
                                VktSuoritusMockGenerator(0).randomOsakokeet(
                                    Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                                    LocalDate.of(2024, 5, 1),
                                    oppilaitos = Oid.parse(ORGANISAATIO).getOrThrow(),
                                ),
                            lahdejarjestelmanId = LahdejarjestelmanTunniste("vkt.0", Lahdejarjestelma.KIOS),
                        ),
                ),
            ).fold(ifLeft = { error("VKT-esimerkki ei kelpaa KOSKI-siirtoon: $it") }, ifRight = { it })

    private fun solkiArvioija() =
        SolkiArvioijaRequest(
            arvioijaOid = oppija.toString(),
            versio = OffsetDateTime.parse("2026-09-16T12:00:00+03:00"),
            tila = SolkiArvioijaRequest.Tila.ARVIOIJA,
            sukunimi = "Keijunen",
            etunimet = "Keijo",
            syntymapaiva = LocalDate.of(1980, 1, 1),
            sahkopostiosoite = "keijo.keijunen@example.com",
            katuosoite = "Esimerkkikatu 1",
            postinumero = "00100",
            postitoimipaikka = "Helsinki",
            ensimmainenRekisterointipaiva = LocalDate.of(2020, 1, 1),
            arviointikausi =
                SolkiArvioijaRequest.Arviointikausi(
                    id = 412,
                    alkupaiva = LocalDate.of(2025, 1, 1),
                    paattymispaiva = LocalDate.of(2029, 12, 31),
                    jatkorekisterointi = true,
                    arviointioikeudet =
                        listOf(
                            SolkiArvioijaRequest.Arviointioikeus(
                                id = 981,
                                kieli = Tutkintokieli.FIN,
                                tasot = listOf(Tutkintotaso.PT, Tutkintotaso.KT),
                            ),
                            SolkiArvioijaRequest.Arviointioikeus(
                                id = 982,
                                kieli = Tutkintokieli.SWE,
                                tasot = listOf(Tutkintotaso.YT),
                            ),
                        ),
                ),
        )

    private fun solkiArvioijanPoisto() =
        SolkiArvioijaPoistoRequest(
            arvioijaOid = oppija.toString(),
            versio = OffsetDateTime.parse("2026-09-16T12:00:00+03:00"),
        )

    private fun kiosArviointitila() =
        YkiArvioinninTilaRequest(
            nonEmptyListOf(
                YkiArvioinninTila(
                    suoritus =
                        YkiSuorituksenTunniste(
                            oppijanumero = oppija,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            tutkintokieli = Tutkintokieli.FIN,
                            tutkintotaso = Tutkintotaso.KT,
                            osakokeet = listOf(TutkinnonOsa.PU, TutkinnonOsa.KI),
                        ),
                    tila = Arviointitila.ARVIOITAVA,
                ),
            ),
        )
}
