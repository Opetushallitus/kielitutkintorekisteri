package fi.oph.kitu.oppijanumero

import org.springframework.stereotype.Service

@Service
class OppijanumeroTroubleshootingService(
    private val oppijanumeroService: OppijanumeroService,
) {
    fun troubleshootOppijaNameCombinations(oppija: Oppija): Oppija? =
        tryEachEtunimiAsKutsumanimi(oppija) ?: switchEtunimetAndSukunimi(oppija)

    fun tryEachEtunimiAsKutsumanimi(oppija: Oppija): Oppija? = kutsumanimivaihtoehdot(oppija).firstOrNull(::loytyy)

    fun henkiloByHetu(hetu: String): OppijanumerorekisteriHenkilo? =
        runCatching { oppijanumeroService.getHenkiloByHetu(hetu).getOrNull() }.getOrNull()

    fun switchEtunimetAndSukunimi(oppija: Oppija): Oppija? = tryEachEtunimiAsKutsumanimi(paittain(oppija))

    fun tryAllNameCombinations(oppija: Oppija): Oppija? {
        val kokeillut =
            (listOf(oppija) + kutsumanimivaihtoehdot(oppija) + kutsumanimivaihtoehdot(paittain(oppija))).toSet()
        return kaikkiNimiyhdistelmat(oppija)
            .filterNot { it in kokeillut }
            .take(LAAJAN_HAUN_ENIMMAISYRITYKSET)
            .firstOrNull(::loytyy)
    }

    private fun loytyy(oppija: Oppija): Boolean = oppijanumeroService.getMasterOid(oppija).isRight()

    private fun kutsumanimivaihtoehdot(oppija: Oppija): List<Oppija> =
        oppija.etunimet.split(" ").map { oppija.copy(kutsumanimi = it) }

    private fun paittain(oppija: Oppija): Oppija = oppija.copy(etunimet = oppija.sukunimi, sukunimi = oppija.etunimet)

    companion object {
        const val LAAJAN_HAUN_ENIMMAISYRITYKSET = 150
        const val LAAJAN_HAUN_ENIMMAISNIMET = 6

        fun kaikkiNimiyhdistelmat(oppija: Oppija): List<Oppija> {
            val etunimet = nimet(oppija.etunimet)
            val nimet = etunimet + nimet(oppija.sukunimi)
            if (nimet.size < 2 || nimet.size > LAAJAN_HAUN_ENIMMAISNIMET) return emptyList()

            return permutaatiot(nimet.indices.toList())
                .flatMap { jarjestys ->
                    (1 until nimet.size).flatMap { raja ->
                        val etu = jarjestys.take(raja)
                        val suku = jarjestys.drop(raja)
                        etu.map { kutsu -> Yhdistelma(etu, suku, kutsu, etunimet.size) }
                    }
                }.sortedWith(
                    compareBy(
                        Yhdistelma::siirretyt,
                        Yhdistelma::etunimienJarjestysmuutokset,
                        Yhdistelma::sukunimienJarjestysmuutokset,
                        { it.kutsumanimenSijainti },
                    ),
                ).map { it.oppija(oppija.hetu, nimet) }
                .distinct()
        }

        private fun nimet(teksti: String): List<String> = teksti.split(Regex("\\s+")).filter { it.isNotEmpty() }

        private fun <T> permutaatiot(lista: List<T>): List<List<T>> =
            if (lista.size <= 1) {
                listOf(lista)
            } else {
                lista.flatMap { eka -> permutaatiot(lista - eka).map { listOf(eka) + it } }
            }

        private fun jarjestysmuutokset(indeksit: List<Int>): Int =
            indeksit.indices.sumOf { i -> (i + 1 until indeksit.size).count { j -> indeksit[i] > indeksit[j] } }

        private data class Yhdistelma(
            val etu: List<Int>,
            val suku: List<Int>,
            val kutsu: Int,
            val etunimiaAlunperin: Int,
        ) {
            val siirretyt = etu.count { it >= etunimiaAlunperin } + suku.count { it < etunimiaAlunperin }
            val etunimienJarjestysmuutokset = jarjestysmuutokset(etu)
            val sukunimienJarjestysmuutokset = jarjestysmuutokset(suku)
            val kutsumanimenSijainti = etu.indexOf(kutsu)

            fun oppija(
                hetu: String,
                nimet: List<String>,
            ) = Oppija(
                etunimet = etu.joinToString(" ") { nimet[it] },
                hetu = hetu,
                kutsumanimi = nimet[kutsu],
                sukunimi = suku.joinToString(" ") { nimet[it] },
            )
        }
    }
}
