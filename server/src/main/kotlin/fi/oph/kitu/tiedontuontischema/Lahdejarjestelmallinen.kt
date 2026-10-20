package fi.oph.kitu.tiedontuontischema

import kotlin.random.Random

interface Lahdejarjestelmallinen {
    val lahdejarjestelmanId: LahdejarjestelmanTunniste
}

data class LahdejarjestelmanTunniste(
    val id: String,
    val lahde: Lahdejarjestelma,
) {
    override fun toString() = "$lahde:$id"

    fun toTunnus() = "${lahde.tunnusPrefix}.$id"

    companion object {
        fun from(s: String): LahdejarjestelmanTunniste {
            val tokens = s.split(":", limit = 2)
            return if (tokens.size > 1) {
                LahdejarjestelmanTunniste(tokens[1], Lahdejarjestelma.valueOf(tokens[0]))
            } else {
                LahdejarjestelmanTunniste(s, Lahdejarjestelma.Unknown)
            }
        }

        fun randomFrom(from: Lahdejarjestelma) = LahdejarjestelmanTunniste(Random.nextInt(1000000).toString(), from)
    }
}

enum class Lahdejarjestelma(
    val tunnusPrefix: String,
) {
    KIOS("kios"),
    Solki("yki"),
    OPH("oph"), // Manuaalisesti lisätyt suoritukset. Tätä vastaavan tunnisteen id on ASHA-diaarinumero.
    OPHTesti("ophtesti"),
    Unknown("tuntematon"),
    ;

    companion object {
        fun ofTunnus(tunnus: String): Lahdejarjestelma =
            entries.firstOrNull { tunnus.startsWith("${it.tunnusPrefix}.") } ?: Unknown
    }
}
