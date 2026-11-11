package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.AtaruLasku
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import java.time.LocalDate

enum class Maksuntila {
    MAKSETTU,
    AVOINNA,
    ERAANTYNYT,
    EI_LASKUA,
    EI_TIEDOSSA,
    ;

    val nimi: LocalizedString
        get() =
            when (this) {
                MAKSETTU -> UiText.Yki.Arvioijahakemus.Maksu.maksettu
                AVOINNA -> UiText.Yki.Arvioijahakemus.Maksu.avoinna
                ERAANTYNYT -> UiText.Yki.Arvioijahakemus.Maksu.eraantynyt
                EI_LASKUA -> UiText.Yki.Arvioijahakemus.Maksu.eiLaskua
                EI_TIEDOSSA -> UiText.Yki.Arvioijahakemus.Maksu.eiTiedossa
            }
}

/** Atarun ASTU-paatosmaksun tila haun hetkella; vain naytettava tieto, portti on atarun `processed`. */
data class Maksutieto(
    val tila: Maksuntila,
    val maksettu: LocalDate? = null,
    val erapaiva: LocalDate? = null,
) {
    companion object {
        val EI_TIEDOSSA = Maksutieto(Maksuntila.EI_TIEDOSSA)

        fun koosta(laskut: List<AtaruLasku>): Maksutieto {
            val paatosmaksut = laskut.filter { it.origin == "astu" }
            paatosmaksut
                .filter { it.status == "paid" }
                .maxByOrNull { paiva(it.paidAt) ?: LocalDate.MIN }
                ?.let {
                    return Maksutieto(
                        Maksuntila.MAKSETTU,
                        maksettu = paiva(it.paidAt),
                        erapaiva = paiva(it.dueDate),
                    )
                }

            val viimeisin =
                paatosmaksut
                    .filter { it.status == "active" || it.status == "overdue" }
                    .maxByOrNull { paiva(it.dueDate) ?: LocalDate.MIN }
                    ?: return Maksutieto(Maksuntila.EI_LASKUA)
            return Maksutieto(
                if (viimeisin.status == "active") Maksuntila.AVOINNA else Maksuntila.ERAANTYNYT,
                erapaiva = paiva(viimeisin.dueDate),
            )
        }

        private fun paiva(arvo: String?): LocalDate? = arvo?.let { runCatching { LocalDate.parse(it) }.getOrNull() }
    }
}
