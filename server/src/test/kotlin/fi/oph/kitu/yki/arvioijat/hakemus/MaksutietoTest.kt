package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.AtaruLasku
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class MaksutietoTest {
    private fun lasku(
        status: String,
        erapaiva: String,
        maksettu: String? = null,
        origin: String = "astu",
    ) = AtaruLasku("tilaus-$erapaiva", status, origin, erapaiva, maksettu)

    @Test
    fun `maksettu lasku voittaa myohemman avoimen laskun`() {
        assertEquals(
            Maksutieto(
                Maksuntila.MAKSETTU,
                maksettu = LocalDate.of(2026, 10, 3),
                erapaiva = LocalDate.of(2026, 10, 15),
            ),
            Maksutieto.koosta(listOf(lasku("active", "2026-12-01"), lasku("paid", "2026-10-15", "2026-10-03"))),
        )
    }

    @Test
    fun `viimeisin avoin tai erääntynyt lasku ratkaisee`() {
        assertEquals(
            Maksutieto(Maksuntila.AVOINNA, erapaiva = LocalDate.of(2026, 12, 1)),
            Maksutieto.koosta(listOf(lasku("overdue", "2026-10-15"), lasku("active", "2026-12-01"))),
        )
        assertEquals(
            Maksutieto(Maksuntila.ERAANTYNYT, erapaiva = LocalDate.of(2026, 10, 15)),
            Maksutieto.koosta(listOf(lasku("overdue", "2026-10-15"), lasku("invalidated", "2026-12-01"))),
        )
    }

    @Test
    fun `ilman voimassa olevaa ASTU-laskua ei ole laskua`() {
        assertEquals(Maksutieto(Maksuntila.EI_LASKUA), Maksutieto.koosta(emptyList()))
        assertEquals(Maksutieto(Maksuntila.EI_LASKUA), Maksutieto.koosta(listOf(lasku("invalidated", "2026-10-15"))))
        assertEquals(
            Maksutieto(Maksuntila.EI_LASKUA),
            Maksutieto.koosta(listOf(lasku("paid", "2026-10-15", "2026-10-03", origin = "tutu"))),
        )
    }
}
