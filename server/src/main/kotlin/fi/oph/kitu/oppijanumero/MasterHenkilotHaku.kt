package fi.oph.kitu.oppijanumero

import com.fasterxml.jackson.annotation.JsonValue

const val MASTERHENKILOT_KERRALLA_ENINTAAN = 5000

data class OidlistaRequest(
    @get:JsonValue
    val oids: List<String>,
) : OppijanumerorekisteriRequest

class MasterHenkilot : LinkedHashMap<String, OppijanumerorekisteriHenkilo>()
