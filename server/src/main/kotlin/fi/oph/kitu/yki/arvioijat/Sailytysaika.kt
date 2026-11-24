package fi.oph.kitu.yki.arvioijat

/**
 * Arvioijarekisterimerkinnan sailytysaika. Laskenta alkaa hetkesta, jona merkinta muuttui
 * passiiviseksi: hallintopaatoksella passivoidulla se on [YkiArvioijaEntity.passivoitu], kauden
 * umpeutumiseen paattyneella kauden viimeinen voimassaolopaiva. Jos kumpaakaan ei ole tiedossa,
 * merkinta ei vanhene lainkaan.
 */
object Sailytysaika {
    const val VUOSIA = 5L

    /**
     * Alkuhetki SQL:na; sama COALESCE molemmissa kyselyissa, jottei suoja ja poisto eroa.
     *
     * yki_arviointioikeus on kielikohtainen projektio kausista ja voi vanhentuneena osoittaa
     * paattyneeseen kauteen vaikka arvioijalla on uudempi kausi. GREATEST ottaa myohaisemman kahdesta lahteesta, joten
     * vanhentunut projektio ei voi aikaistaa sailytysajan alkua eika siten poistaa voimassa
     * olevaa merkintaa.
     */
    const val ALKUHETKI_SQL = """
        COALESCE(
            yki_arvioija.passivoitu::date,
            GREATEST(
                (
                    SELECT max(kauden_paattymispaiva)
                    FROM yki_arviointioikeus
                    WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id
                ),
                (
                    SELECT max(paattymispaiva)
                    FROM yki_arvioija_arviointikausi
                    WHERE yki_arvioija_arviointikausi.arvioija_id = yki_arvioija.id
                )
            )
        )
    """
}
