package fi.oph.kitu.tiedontuontischema

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.yki.Sukupuoli

data class Henkilo(
    val oid: Oid,
    val etunimet: String? = null,
    val sukunimi: String? = null,
    val hetu: String? = null,
    val sukupuoli: Sukupuoli? = null,
    val kansalaisuus: String? = null,
    val katuosoite: String? = null,
    val postinumero: String? = null,
    val postitoimipaikka: String? = null,
    val maa: String? = null,
    val email: String? = null,
) {
    fun kokoNimi(): String = "$sukunimi, $etunimet"
}
