package fi.oph.kitu.i18n

enum class Language {
    FI,
    SV,
    EN,
}

data class LocalizedString(
    val fi: String? = null,
    val sv: String? = null,
    val en: String? = null,
) {
    private var tolgeeKey: String? = null

    override fun toString(): String = get(CurrentLanguage.get())

    fun get(lang: Language): String = resolve(lang) ?: resolve(Language.FI) ?: tolgeeKey ?: "<invalid LocalizedString>"

    fun contains(
        other: CharSequence,
        ignoreCase: Boolean = false,
    ): Boolean = Language.entries.mapNotNull { resolve(it) }.any { it.contains(other, ignoreCase) }

    fun interpolate(vararg args: Pair<String, Any?>): LocalizedString {
        fun substitute(text: String?): String? =
            args.fold(text) { acc, (name, value) -> acc?.replace("{$name}", value.toString()) }
        return LocalizedString(
            fi = substitute(resolve(Language.FI)) ?: tolgeeKey,
            sv = substitute(resolve(Language.SV)),
            en = substitute(resolve(Language.EN)),
        )
    }

    override fun equals(other: Any?): Boolean =
        other is LocalizedString &&
            fi == other.fi &&
            sv == other.sv &&
            en == other.en &&
            tolgeeKey == other.tolgeeKey

    override fun hashCode(): Int = listOf(fi, sv, en, tolgeeKey).hashCode()

    private fun resolve(lang: Language): String? {
        val tolgee = tolgeeKey?.let { TolgeeMessages.get(it) }
        return when (lang) {
            Language.FI -> tolgee?.fi ?: fi
            Language.SV -> tolgee?.sv ?: sv
            Language.EN -> tolgee?.en ?: en
        }
    }

    companion object {
        fun withTolgeeKey(key: String): LocalizedString = LocalizedString().also { it.tolgeeKey = key }
    }
}

/**
 * Yhdistää kielikohtaiset litteät käännöskartat avainkohtaisiksi [LocalizedString]eiksi.
 * Yhteinen sekä lokalisointipalvelun haulle että luokkapolun lähdeteksteille, jottei
 * kahta yhdistelysääntöä pääse syntymään.
 */
internal fun Map<Language, Map<String, String>>.yhdistaKaannokset(): Map<String, LocalizedString> =
    values
        .flatMap { it.keys }
        .toSet()
        .associateWith { key ->
            LocalizedString(
                fi = this[Language.FI]?.get(key),
                sv = this[Language.SV]?.get(key),
                en = this[Language.EN]?.get(key),
            )
        }
