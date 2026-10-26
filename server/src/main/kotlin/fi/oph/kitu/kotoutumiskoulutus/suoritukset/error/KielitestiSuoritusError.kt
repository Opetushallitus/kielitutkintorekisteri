package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.oid.Oid
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant

@Table(name = "koto_suoritus_error")
data class KielitestiSuoritusError(
    @Id
    val id: Long?,
    val suorittajanOid: String?,
    val hetu: String?,
    val nimi: String,
    val etunimet: String,
    val sukunimi: String,
    val kutsumanimi: String?,
    val schoolOid: Oid?,
    @Transient
    val organisaatio: LocalizedString? = null,
    val teacherEmail: String?,
    val virheenLuontiaika: Instant,
    val viesti: String,
    val virheellinenKentta: String?,
    val virheellinenArvo: String?,
    val lisatietoja: String?,
    val onrLisatietoja: String? = null,
    val onrEtunimet: String? = null,
    val onrKutsumanimi: String? = null,
    val onrSukunimi: String? = null,
    val onrStatus: Int? = null,
    val kurssiId: Int? = null,
    val kurssi: String? = null,
    val suoritusaika: Instant? = null,
    val completed: Boolean = true,
)
