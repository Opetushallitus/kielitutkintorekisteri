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

    fun get(lang: Language): String = resolve(lang) ?: resolve(Language.FI) ?: "<invalid LocalizedString>"

    fun contains(
        other: CharSequence,
        ignoreCase: Boolean = false,
    ): Boolean = Language.entries.mapNotNull { resolve(it) }.any { it.contains(other, ignoreCase) }

    fun interpolate(vararg args: Pair<String, Any?>): LocalizedString {
        fun substitute(text: String?): String? =
            args.fold(text) { acc, (name, value) -> acc?.replace("{$name}", value.toString()) }
        return LocalizedString(
            fi = substitute(resolve(Language.FI)),
            sv = substitute(resolve(Language.SV)),
            en = substitute(resolve(Language.EN)),
        )
    }

    private fun resolve(lang: Language): String? {
        val tolgee = tolgeeKey?.let { TolgeeMessages.get(it) }
        return when (lang) {
            Language.FI -> tolgee?.fi ?: fi
            Language.SV -> tolgee?.sv ?: sv
            Language.EN -> tolgee?.en ?: en
        }
    }

    companion object {
        fun withTolgeeKey(
            key: String,
            fi: String,
        ): LocalizedString = LocalizedString(fi = fi).also { it.tolgeeKey = key }
    }
}
