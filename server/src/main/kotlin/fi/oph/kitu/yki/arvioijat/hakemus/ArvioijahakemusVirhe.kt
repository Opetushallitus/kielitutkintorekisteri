package fi.oph.kitu.yki.arvioijat.hakemus

import com.fasterxml.jackson.annotation.JsonInclude
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.jdbc.core.RowMapper
import tools.jackson.databind.JsonNode
import tools.jackson.module.kotlin.readValue
import java.time.OffsetDateTime

enum class ArvioijahakemuksenVirhetyyppi {
    VIRHEELLINEN_RAKENNE,
    TUNTEMATON_VAIHTOEHTO,
    EI_KIELIA,
    KIELELTA_PUUTTUU_TASO,
    VAARA_TIETOTYYPPI,
    VALIDOINTIVIRHE,
    HENKILOA_EI_LOYDY,
    OSOITE_PUUTTUU,
    ;

    val nimi: LocalizedString
        get() =
            when (this) {
                VIRHEELLINEN_RAKENNE -> UiText.Yki.Arvioijahakemus.Virhe.virheellinenRakenne
                TUNTEMATON_VAIHTOEHTO -> UiText.Yki.Arvioijahakemus.Virhe.tuntematonVaihtoehto
                EI_KIELIA -> UiText.Yki.Arvioijahakemus.Virhe.eiKielia
                KIELELTA_PUUTTUU_TASO -> UiText.Yki.Arvioijahakemus.Virhe.kieleltaPuuttuuTaso
                VAARA_TIETOTYYPPI -> UiText.Yki.Arvioijahakemus.Virhe.vaaraTietotyyppi
                VALIDOINTIVIRHE -> UiText.Yki.Arvioijahakemus.Virhe.validointivirhe
                HENKILOA_EI_LOYDY -> UiText.Yki.Arvioijahakemus.Virhe.henkiloaEiLoydy
                OSOITE_PUUTTUU -> UiText.Yki.Arvioijahakemus.Virhe.osoitePuuttuu
            }
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ArvioijahakemuksenVirhe(
    val tyyppi: ArvioijahakemuksenVirhetyyppi,
    val kentta: String? = null,
    val arvo: JsonNode? = null,
    val viesti: String? = null,
) {
    fun tallennettava(): ArvioijahakemuksenVirhe =
        copy(
            arvo =
                arvo?.takeUnless {
                    val teksti = it.toString()
                    tyyppi == ArvioijahakemuksenVirhetyyppi.VIRHEELLINEN_RAKENNE ||
                        kentta in EI_TALLENNETTAVAT_KENTAT ||
                        teksti.length > ARVON_ENIMMAISPITUUS ||
                        HETU.containsMatchIn(teksti)
                },
            viesti = viesti?.takeUnless { HETU.containsMatchIn(it) },
        )

    companion object {
        private val EI_TALLENNETTAVAT_KENTAT = setOf("ssn", "birth-date")
        private const val ARVON_ENIMMAISPITUUS = 500
        private val HETU = Regex("""\d{6}[-+A-FU-Y]\d{3}[0-9A-FHJ-NPR-Y]""", RegexOption.IGNORE_CASE)
    }
}

data class ArvioijahakemusVirheEntity(
    val hakemusOid: String,
    val henkiloOid: String?,
    val virheet: List<ArvioijahakemuksenVirhe>,
    val ensimmainenYritys: OffsetDateTime,
    val viimeisinYritys: OffsetDateTime,
    val yrityksia: Int,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                ArvioijahakemusVirheEntity(
                    hakemusOid = rs.getString("hakemus_oid"),
                    henkiloOid = rs.getString("henkilo_oid"),
                    virheet = defaultObjectMapper.readValue<List<ArvioijahakemuksenVirhe>>(rs.getString("virheet")),
                    ensimmainenYritys = rs.getObject("ensimmainen_yritys", OffsetDateTime::class.java),
                    viimeisinYritys = rs.getObject("viimeisin_yritys", OffsetDateTime::class.java),
                    yrityksia = rs.getInt("yrityksia"),
                )
            }
    }
}
