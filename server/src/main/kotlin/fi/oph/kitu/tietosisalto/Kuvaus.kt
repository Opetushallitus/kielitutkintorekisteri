package fi.oph.kitu.tietosisalto

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Kuvaus(
    val teksti: String,
    val lisatieto: String = "",
)
