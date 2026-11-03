package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.yki.Arviointitila

sealed interface PoistaSuoritusError {
    val message: String

    data class SuoritustaEiLoydy(
        val tunniste: LahdejarjestelmanTunniste,
    ) : PoistaSuoritusError {
        override val message: String
            get() = "Suoritusta ei löydy tunnisteella $tunniste"
    }

    data class TilaEstaaPoiston(
        val tunniste: LahdejarjestelmanTunniste,
        val arviointitila: Arviointitila,
    ) : PoistaSuoritusError {
        override val message: String
            get() = "Suoritusta $tunniste ei voi poistaa, koska sen arviointitila on $arviointitila"
    }
}
