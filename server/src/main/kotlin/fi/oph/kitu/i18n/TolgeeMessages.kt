package fi.oph.kitu.i18n

object TolgeeMessages {
    @Volatile
    private var store: Map<String, LocalizedString> = emptyMap()

    fun get(key: String): LocalizedString? = store[key]

    fun set(messages: Map<String, LocalizedString>) {
        store = messages
    }

    fun clear() = set(emptyMap())
}
