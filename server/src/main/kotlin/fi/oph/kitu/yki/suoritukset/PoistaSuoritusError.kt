package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.yki.Arviointitila

sealed interface PoistaSuoritusError {
    val message: String

    data class SuoritustaEiLoydy(
        val tunnus: String,
    ) : PoistaSuoritusError {
        override val message: String
            get() = "Suoritusta ei löydy tunnisteella $tunnus"
    }

    data class TilaEstaaPoiston(
        val tunnus: String,
        val arviointitila: Arviointitila,
    ) : PoistaSuoritusError {
        override val message: String
            get() = "Suoritusta $tunnus ei voi poistaa, koska sen arviointitila on $arviointitila"
    }
}
