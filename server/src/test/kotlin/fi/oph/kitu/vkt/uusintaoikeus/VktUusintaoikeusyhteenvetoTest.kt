package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.Koodisto.VktTaitotaso.Erinomainen
import fi.oph.kitu.koodisto.Koodisto.VktTaitotaso.HyväJaTyydyttävä
import fi.oph.kitu.vkt.uusintaoikeus.VktUusintaoikeusyhteenveto.Maarat
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class VktUusintaoikeusyhteenvetoTest {
    @Test
    fun `henkilot ja uusittavat osakokeet lasketaan erikseen taitotasoittain`() {
        val yhteenveto =
            VktUusintaoikeusyhteenveto.laske(
                listOf(
                    rivi("1.2.246.562.24.00000000001", HyväJaTyydyttävä, Koodisto.VktOsakoe.Kirjoittaminen),
                    rivi("1.2.246.562.24.00000000001", HyväJaTyydyttävä, Koodisto.VktOsakoe.Puhuminen),
                    rivi("1.2.246.562.24.00000000001", Erinomainen, Koodisto.VktOsakoe.Kirjoittaminen),
                    rivi("1.2.246.562.24.00000000002", HyväJaTyydyttävä, Koodisto.VktOsakoe.Kirjoittaminen),
                ),
            )

        assertEquals(
            VktUusintaoikeusyhteenveto(
                taitotasoittain =
                    mapOf(
                        Erinomainen to Maarat(henkiloita = 1, osakokeita = 1),
                        HyväJaTyydyttävä to Maarat(henkiloita = 2, osakokeita = 3),
                    ),
                yhteensa = Maarat(henkiloita = 2, osakokeita = 4),
            ),
            yhteenveto,
        )
    }

    @Test
    fun `tyhja raportti antaa nollat kaikille taitotasoille`() {
        assertEquals(
            VktUusintaoikeusyhteenveto(
                taitotasoittain =
                    mapOf(
                        Erinomainen to Maarat(henkiloita = 0, osakokeita = 0),
                        HyväJaTyydyttävä to Maarat(henkiloita = 0, osakokeita = 0),
                    ),
                yhteensa = Maarat(henkiloita = 0, osakokeita = 0),
            ),
            VktUusintaoikeusyhteenveto.laske(emptyList()),
        )
    }

    private fun rivi(
        oid: String,
        taitotaso: Koodisto.VktTaitotaso,
        osakoe: Koodisto.VktOsakoe,
    ) = VktUusintaoikeus(
        suorittajanOid = oid,
        sukunimi = "Testinen",
        etunimet = "Testi",
        tutkintokieli = Koodisto.Tutkintokieli.FIN,
        taitotaso = taitotaso,
        osakoe = osakoe,
        tutkinnot = listOf(Koodisto.VktKielitaito.Kirjallinen),
        hylkaamispaatos = LocalDate.of(2025, 3, 15),
        oikeusPaattyy = LocalDate.of(2028, 3, 15),
    )
}
