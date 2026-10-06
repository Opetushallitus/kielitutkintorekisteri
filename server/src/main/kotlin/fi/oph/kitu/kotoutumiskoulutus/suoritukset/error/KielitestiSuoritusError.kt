package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import fi.oph.kitu.oid.Oid
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant

@JsonPropertyOrder(
    "virheenLuontiaika",
    "suorittajanOid",
    "hetu",
    "nimi",
    "etunimet",
    "sukunimi",
    "kutsumanimi",
    "schoolOid",
    "teacherEmail",
    "viesti",
    "lisatietoja",
    "onrLisatietoja",
    "onrEtunimet",
    "onrKutsumanimi",
    "onrSukunimi",
    "virheellinenKentta",
    "virheellinenArvo",
)
@Table(name = "koto_suoritus_error")
data class KielitestiSuoritusError(
    @JsonIgnore
    @Id
    val id: Long?,
    val suorittajanOid: String?,
    val hetu: String?,
    val nimi: String,
    val etunimet: String,
    val sukunimi: String,
    val kutsumanimi: String?,
    val schoolOid: Oid?,
    val teacherEmail: String?,
    @param:JsonProperty("virheenLuontiaika")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssX", timezone = "UTC")
    val virheenLuontiaika: Instant,
    val viesti: String,
    val virheellinenKentta: String?,
    val virheellinenArvo: String?,
    val lisatietoja: String?,
    val onrLisatietoja: String? = null,
    val onrEtunimet: String? = null,
    val onrKutsumanimi: String? = null,
    val onrSukunimi: String? = null,
    @JsonIgnore
    val completed: Boolean = true,
)
