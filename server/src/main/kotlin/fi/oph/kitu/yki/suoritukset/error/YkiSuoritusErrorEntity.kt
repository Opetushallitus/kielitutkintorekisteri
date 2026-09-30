package fi.oph.kitu.yki.suoritukset.error

import fi.oph.kitu.jdbc.getInstant
import fi.oph.kitu.jdbc.getInstantOrNull
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.sql.ResultSet
import java.time.Instant

/**
 * Represents Yki Suoritus Error Entity.
 *
 * virheelllinenRivi and virheenRivinumero forms a unique constraint.
 */
@Table(name = "yki_suoritus_error")
data class YkiSuoritusErrorEntity(
    @Id
    val id: Long?,
    val suorittajanOid: String?,
    val hetu: String?,
    val nimi: String?,
    val lastModified: Instant?,
    val virheellinenKentta: String?,
    val virheellinenArvo: String?,
    val virheellinenRivi: String,
    val virheenRivinumero: Int,
    val virheenLuontiaika: Instant,
) {
    companion object
}

fun YkiSuoritusErrorEntity.Companion.fromResultSet(rs: ResultSet): YkiSuoritusErrorEntity =
    YkiSuoritusErrorEntity(
        rs.getLong("id"),
        rs.getString("suorittajan_oid"),
        rs.getString("hetu"),
        rs.getString("nimi"),
        rs.getInstantOrNull("last_modified"),
        rs.getString("virheellinen_kentta"),
        rs.getString("virheellinen_arvo"),
        rs.getString("virheellinen_rivi"),
        rs.getInt("virheen_rivinumero"),
        rs.getInstant("virheen_luontiaika"),
    )
