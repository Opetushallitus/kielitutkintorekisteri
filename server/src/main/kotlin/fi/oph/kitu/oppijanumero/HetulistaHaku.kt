package fi.oph.kitu.oppijanumero

import com.fasterxml.jackson.annotation.JsonValue

/**
 * Oppijanumerorekisterin `henkilo/henkiloPerustietosByHenkiloHetuList` ottaa vastaan
 * pelkän listan henkilötunnuksia, joten pyyntö sarjallistuu JSON-taulukoksi.
 */
data class HetulistaRequest(
    @get:JsonValue
    val hetut: List<String>,
) : OppijanumerorekisteriRequest

data class OppijanumerorekisteriPerustieto(
    val oidHenkilo: String?,
    val hetu: String?,
)
