package fi.oph.kitu.yki.arvioijat.solki

import com.fasterxml.jackson.annotation.JsonFormat
import fi.oph.kitu.tietosisalto.Kuvaus
import java.time.OffsetDateTime

/**
 * Ilmoitus Kielitutkintorekisterista poistetusta arvioijasta. Oma luokkansa, jotta
 * [SolkiArvioijaRequest]in pakolliset kentat pysyvat pakollisina: poistetusta henkilosta ei
 * laheteta nimea eika osoitetta.
 */
data class SolkiArvioijaPoistoRequest(
    @Kuvaus("Poistetun arvioijan oppijanumero")
    val arvioijaOid: String,
    @Kuvaus("Hetki, jona arvioija poistettiin Kielitutkintorekisteristä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING)
    val versio: OffsetDateTime,
    @Kuvaus("Arvioijan tila")
    val tila: SolkiArvioijaRequest.Tila = SolkiArvioijaRequest.Tila.POISTETTU,
)
