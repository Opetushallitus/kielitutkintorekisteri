package fi.oph.kitu.util.validation

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.suoritukset.Todistuskieli
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest(properties = ["kitu.yki.convertLegacyArviointitila.enabled=true"])
@Import(DBContainerConfiguration::class)
class YkiArviointitilanMuunnosTest(
    @param:Autowired val validation: ValidationService,
) {
    private fun suoritus(
        arviointitila: Arviointitila,
        arvosanat: List<Int?>,
        arviointipaiva: LocalDate? = LocalDate.of(2020, 1, 1),
    ) = Henkilosuoritus(
        henkilo = Henkilo(oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(), hetu = "010180-9026"),
        suoritus =
            YkiSuoritus(
                tutkintotaso = Tutkintotaso.KT,
                kieli = Tutkintokieli.FIN,
                todistuskieli = Todistuskieli.FIN,
                jarjestaja =
                    YkiJarjestaja(
                        oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                        nimi = "Soveltavan kielentutkimuksen keskus",
                    ),
                tutkintopaiva = LocalDate.of(2020, 1, 1),
                arviointipaiva = arviointipaiva,
                arviointitila = arviointitila,
                osat =
                    arvosanat.mapIndexed { i, arvosana ->
                        YkiOsa(tyyppi = osatyypit[i], arvosana = arvosana)
                    },
                tarkistusarviointi = null,
                lahdejarjestelmanId = LahdejarjestelmanTunniste(id = "666", lahde = Lahdejarjestelma.Solki),
            ),
    )

    private fun muunnettuTila(hs: Henkilosuoritus<YkiSuoritus>): Arviointitila =
        validation
            .validateAndEnrich(hs)
            .getOrThrow()
            .suoritus.arviointitila

    @Test
    fun `ARVIOITU ilman oikeita arvosanoja muunnetaan EI_SUORITUSTA-tilaan`() {
        assertEquals(
            Arviointitila.EI_SUORITUSTA,
            muunnettuTila(suoritus(Arviointitila.ARVIOITU, listOf(9, 9))),
        )
    }

    @Test
    fun `ARVIOITU jossa on oikea arvosana säilyy ARVIOITU-tilassa`() {
        assertEquals(
            Arviointitila.ARVIOITU,
            muunnettuTila(suoritus(Arviointitila.ARVIOITU, listOf(3, 3))),
        )
    }

    @Test
    fun `ILMOITTAUTUNUT säilyy kun osakokeilla ei ole arvosanaa`() {
        assertEquals(
            Arviointitila.ILMOITTAUTUNUT,
            muunnettuTila(suoritus(Arviointitila.ILMOITTAUTUNUT, listOf(null, null), arviointipaiva = null)),
        )
    }

    companion object {
        private val osatyypit = listOf(TutkinnonOsa.puheenYmmartaminen, TutkinnonOsa.puhuminen)
    }
}
