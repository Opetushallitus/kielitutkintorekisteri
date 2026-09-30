package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.getEnumOrNull
import fi.oph.kitu.jdbc.getInstant
import fi.oph.kitu.jdbc.getInstantOrNull
import fi.oph.kitu.jdbc.sortedWithDirectionBy
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.organisaatiot.Organisaatiot
import fi.oph.kitu.util.IgnoreForEquality
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.RowMapper
import java.time.Instant

@Table("koto_suoritus")
data class KielitestiSuoritus(
    @Id
    @IgnoreForEquality("KOTO")
    val id: Int? = null,
    val etunimet: String,
    val sukunimi: String,
    val kutsumanimi: String,
    val oppijanumero: Oid?,
    val email: String,
    val suoritusaika: Instant?,
    val oppilaitosOid: Oid?,
    @Transient
    val oppilaitos: String? = null,
    val opettajanEmail: String?,
    val kurssiId: Int,
    val kurssi: String,
    val luetunYmmartaminen: Arvosana?,
    val kuullunYmmartaminen: Arvosana?,
    val puhe: Arvosana?,
    val kirjoittaminen: Arvosana?,
    val testikieli: Testikieli?,
    @IgnoreForEquality("KOTO")
    val lastModified: Instant = Instant.now(),
    val tehtavapaketti: String?,
    val completed: Boolean = true,
) {
    companion object {
        val fromRow: RowMapper<KielitestiSuoritus> =
            RowMapper { rs, _ ->
                KielitestiSuoritus(
                    id = rs.getInt("id"),
                    etunimet = rs.getString("etunimet"),
                    sukunimi = rs.getString("sukunimi"),
                    kutsumanimi = rs.getString("kutsumanimi"),
                    oppijanumero = rs.getString("oppijanumero")?.let { Oid.parse(it).getOrThrow() },
                    email = rs.getString("email"),
                    suoritusaika = rs.getInstantOrNull("suoritusaika"),
                    oppilaitosOid = Oid.parse(rs.getString("oppilaitos_oid")).getOrThrow(),
                    opettajanEmail = rs.getString("opettajan_email"),
                    kurssiId = rs.getInt("kurssi_id"),
                    kurssi = rs.getString("kurssi"),
                    luetunYmmartaminen = rs.getEnumOrNull<Arvosana>("luetun_ymmartaminen"),
                    kuullunYmmartaminen = rs.getEnumOrNull<Arvosana>("kuullun_ymmartaminen"),
                    puhe = rs.getEnumOrNull<Arvosana>("puhe"),
                    kirjoittaminen = rs.getEnumOrNull<Arvosana>("kirjoittaminen"),
                    testikieli = rs.getEnumOrNull<Testikieli>("testikieli"),
                    lastModified = rs.getInstant("last_modified"),
                    tehtavapaketti = rs.getString("tehtavapaketti"),
                    completed = rs.getBoolean("completed"),
                )
            }
    }
}

fun List<KielitestiSuoritus>.sortByOrgName(
    sortDirection: SortDirection,
    organisaatiot: Organisaatiot,
) = this.sortedWithDirectionBy(sortDirection) { row ->
    organisaatiot.nimet[row.oppilaitosOid]?.fi ?: row.oppilaitosOid.toString()
}
