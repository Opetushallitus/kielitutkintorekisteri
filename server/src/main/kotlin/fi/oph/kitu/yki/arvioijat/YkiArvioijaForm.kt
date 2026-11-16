package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate
import java.time.OffsetDateTime

data class ArvioijaHakuFormData(
    val oppijanumero: String? = null,
)

data class ArvioijaFormData(
    val arvioijaOid: String? = null,
    val sukunimi: String? = null,
    val etunimet: String? = null,
    val sahkopostiosoite: String? = null,
    val puhelinnumero: String? = null,
    val katuosoite: String? = null,
    val postinumero: String? = null,
    val postitoimipaikka: String? = null,
    val kaudenAlkupaiva: LocalDate? = null,
    val ashaNumero: String? = null,
    val turvakielto: Turvakieltotieto = Turvakieltotieto.EI,
    val onOlemassa: Boolean = false,
    /** Optimistisen lukituksen tunniste: mika rivin muokkaushetki oli lomaketta avattaessa. */
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    val muokattu: OffsetDateTime? = null,
    val arviointioikeus: List<String>? = null,
) {
    fun laskettuPaattymispaiva(): LocalDate? = kaudenAlkupaiva?.let(Arviointikausi::paattymispaiva)

    fun arviointioikeudet(): List<TallennaArvioija.Arviointioikeus> =
        arviointioikeus
            .orEmpty()
            .mapNotNull(::parseValinta)
            .groupBy({ it.first }, { it.second })
            .map { (kieli, tasot) -> TallennaArvioija.Arviointioikeus(kieli, tasot.toSet()) }

    fun toPaivitys(oid: Oid): PaivitaArvioijanTiedot =
        PaivitaArvioijanTiedot(
            arvioijaOid = oid,
            sukunimi = sukunimi.orEmpty().trim(),
            etunimet = etunimet.orEmpty().trim(),
            sahkopostiosoite = sahkopostiosoite?.trim()?.takeIf { it.isNotEmpty() },
            puhelinnumero = puhelinnumero?.trim()?.takeIf { it.isNotEmpty() },
            katuosoite = katuosoite.orEmpty().trim(),
            postinumero = postinumero.orEmpty().trim(),
            postitoimipaikka = postitoimipaikka.orEmpty().trim(),
        )

    fun toCommand(
        oid: Oid,
        alkupaiva: LocalDate,
    ): TallennaArvioija =
        TallennaArvioija(
            arvioijaOid = oid,
            sukunimi = sukunimi.orEmpty().trim(),
            etunimet = etunimet.orEmpty().trim(),
            sahkopostiosoite = sahkopostiosoite?.trim()?.takeIf { it.isNotEmpty() },
            puhelinnumero = puhelinnumero?.trim()?.takeIf { it.isNotEmpty() },
            katuosoite = katuosoite.orEmpty().trim(),
            postinumero = postinumero.orEmpty().trim(),
            postitoimipaikka = postitoimipaikka.orEmpty().trim(),
            kaudenAlkupaiva = alkupaiva,
            ashaNumero = ashaNumero?.trim()?.takeIf { it.isNotEmpty() },
            arviointioikeudet = arviointioikeudet(),
        )

    companion object {
        fun of(esitaytto: ArvioijanEsitaytto): ArvioijaFormData {
            val merkinta = esitaytto.olemassaolevaMerkinta
            val pohja =
                merkinta?.let { of(it, esitaytto.turvakielto) }
                    ?: ArvioijaFormData(turvakielto = esitaytto.turvakielto)

            // ONR omistaa henkilotiedot, mutta tyhja ONR-kentta ei saa pyyhkia rekisterin arvoa.
            return pohja.copy(
                arvioijaOid = esitaytto.arvioijaOid.toString(),
                sukunimi = esitaytto.sukunimi.tai(pohja.sukunimi),
                etunimet = esitaytto.etunimet.tai(pohja.etunimet),
                sahkopostiosoite = esitaytto.sahkopostiosoite.tai(pohja.sahkopostiosoite),
                puhelinnumero = esitaytto.puhelinnumero.tai(pohja.puhelinnumero),
                katuosoite = esitaytto.katuosoite.tai(pohja.katuosoite),
                postinumero = esitaytto.postinumero.tai(pohja.postinumero),
                postitoimipaikka = esitaytto.postitoimipaikka.tai(pohja.postitoimipaikka),
                onOlemassa = merkinta != null,
            )
        }

        private fun String?.tai(vara: String?): String? = this?.takeIf { it.isNotBlank() } ?: vara

        fun of(
            arvioija: YkiArvioijaEntity,
            turvakielto: Turvakieltotieto = Turvakieltotieto.EI,
        ): ArvioijaFormData {
            val oikeudet = arvioija.arviointioikeudet
            return ArvioijaFormData(
                arvioijaOid = arvioija.arvioijaOid.toString(),
                sukunimi = arvioija.sukunimi,
                etunimet = arvioija.etunimet,
                sahkopostiosoite = arvioija.sahkopostiosoite,
                puhelinnumero = arvioija.puhelinnumero,
                katuosoite = arvioija.katuosoite,
                postinumero = arvioija.postinumero,
                postitoimipaikka = arvioija.postitoimipaikka,
                kaudenAlkupaiva = oikeudet.firstNotNullOfOrNull { it.kaudenAlkupaiva },
                turvakielto = turvakielto,
                onOlemassa = true,
                muokattu = arvioija.muokattu,
                arviointioikeus =
                    oikeudet.flatMap { oikeus ->
                        oikeus.tasot.map { taso -> valinta(oikeus.kieli, taso) }
                    },
            )
        }

        fun valinta(
            kieli: Tutkintokieli,
            taso: Tutkintotaso,
        ): String = Arviointioikeusvalinta.koodaa(kieli, taso)

        private fun parseValinta(arvo: String): Pair<Tutkintokieli, Tutkintotaso>? = Arviointioikeusvalinta.pura(arvo)
    }
}

/** Arviointioikeusmatriisin valintaruudun arvo, esim. `"FIN:PT"`. */
object Arviointioikeusvalinta {
    fun koodaa(
        kieli: Tutkintokieli,
        taso: Tutkintotaso,
    ): String = "${kieli.name}:${taso.name}"

    fun pura(arvo: String): Pair<Tutkintokieli, Tutkintotaso>? {
        val osat = arvo.split(":")
        if (osat.size != 2) return null
        val kieli = Tutkintokieli.entries.firstOrNull { it.name == osat[0] } ?: return null
        val taso = Tutkintotaso.entries.firstOrNull { it.name == osat[1] } ?: return null
        return kieli to taso
    }

    fun oikeudet(valinnat: List<String>?): List<Kausioikeus> =
        valinnat
            .orEmpty()
            .mapNotNull(::pura)
            .groupBy({ it.first }, { it.second })
            .map { (kieli, tasot) -> Kausioikeus(kieli, tasot.toSet()) }
}

data class KausiFormData(
    val alkupaiva: LocalDate? = null,
    val arviointioikeus: List<String>? = null,
    val ashaNumero: String? = null,
) {
    fun ashaNumeroTrimmattuna(): String? = ashaNumero?.trim()?.takeIf { it.isNotEmpty() }

    fun laskettuPaattymispaiva(): LocalDate? = alkupaiva?.let(Arviointikausi::paattymispaiva)

    fun arviointioikeudet(): List<Kausioikeus> = Arviointioikeusvalinta.oikeudet(arviointioikeus)

    companion object {
        fun of(kausi: YkiArviointikausiEntity): KausiFormData =
            KausiFormData(
                alkupaiva = kausi.alkupaiva,
                ashaNumero = kausi.ashaNumero,
                arviointioikeus =
                    kausi.oikeudet.flatMap { oikeus ->
                        oikeus.tasot.map { taso -> Arviointioikeusvalinta.koodaa(oikeus.kieli, taso) }
                    },
            )
    }
}
