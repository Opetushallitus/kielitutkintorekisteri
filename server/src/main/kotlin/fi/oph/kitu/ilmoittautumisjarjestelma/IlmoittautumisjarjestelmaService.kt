package fi.oph.kitu.ilmoittautumisjarjestelma

import arrow.core.Either
import fi.oph.kitu.util.retry.RetryOutboundIntegration
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.stereotype.Service

interface IlmoittautumisjarjestelmaService {
    fun sendAllUpdatedArvioinninTilat()

    fun sendArvioinninTila(suoritus: YkiSuoritusEntity)
}

@Service
@ConditionalOnBean(IlmoittautumisjarjestelmaClient::class)
class IlmoittautumisjarjestelmaServiceImpl(
    val suoritusRepository: YkiSuoritusRepository,
    val client: IlmoittautumisjarjestelmaClient,
) : IlmoittautumisjarjestelmaService {
    @WithSpan
    override fun sendAllUpdatedArvioinninTilat() {
        val suoritukset =
            suoritusRepository
                .findSuorituksetWithUnsentArvioinninTila()
                .filter { it.arviointitilanLahetysvirhe != "SUORITUSTA_EI_LOYDY" }

        suoritukset.chunked(BATCH_SIZE).forEach { batch ->
            YkiArvioinninTilaRequest.of(batch)?.let { request ->
                val response = sendArvioinninTilat(request)
                saveResponse(batch, response)
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 10
    }

    @WithSpan
    override fun sendArvioinninTila(suoritus: YkiSuoritusEntity) {
        YkiArvioinninTilaRequest.of(suoritus)?.let { request ->
            val response = sendArvioinninTilat(request)
            saveResponse(listOf(suoritus), response)
        }
    }

    @WithSpan
    @RetryOutboundIntegration
    private fun sendArvioinninTilat(
        request: YkiArvioinninTilaRequest,
    ): Either<IlmoittautumisjarjestelmaException, IlmoittautumisjarjestelmaResponse> =
        client.post(
            "yki/v2/api/oauth2/registration/evaluation",
            request,
            IlmoittautumisjarjestelmaResponse::class.java,
        )

    private fun saveResponse(
        suoritukset: List<YkiSuoritusEntity>,
        response: Either<IlmoittautumisjarjestelmaException, IlmoittautumisjarjestelmaResponse>,
    ) = response.fold(
        ifRight = { ok ->
            val virheIds =
                ok.virheet
                    ?.let { virheet ->
                        val tunnisteToSolkiIdMap =
                            suoritukset
                                .mapNotNull { suoritus ->
                                    YkiSuorituksenTunniste.of(suoritus)?.let { it to suoritus.solkiId }
                                }.toMap()
                        virheet
                            .flatMap { virhe ->
                                tunnisteToSolkiIdMap
                                    .filterKeys { tunniste -> virhe.suoritus == tunniste }
                                    .map { it.value to virhe.virhe }
                            }.toMap()
                    }.orEmpty()

            val okSuoritukset = suoritukset.filterNot { virheIds.containsKey(it.solkiId) }

            okSuoritukset.forEach { suoritus ->
                suoritusRepository.setArvioinninTilaSent(suoritus.solkiId)
            }
            virheIds.forEach { (solkiId, virhe) ->
                suoritusRepository.setArvioinninTilanLahetysvirhe(solkiId, virhe)
            }

            Span
                .current()
                .setAttribute("suoritukset.success", okSuoritukset.map { it.solkiId }.joinToString(", "))
                .setAttribute("suoritukset.fail", virheIds.entries.joinToString(", ") { "${it.key}=${it.value}" })
        },
        ifLeft = { exception ->
            suoritukset.forEach { suoritus ->
                suoritusRepository.setArvioinninTilanLahetysvirhe(
                    suoritus.solkiId,
                    exception.debugString(),
                )
            }
        },
    )
}

@Service
@ConditionalOnMissingBean(IlmoittautumisjarjestelmaClient::class)
class IlmoittautumisjarjestelmaServiceMock : IlmoittautumisjarjestelmaService {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    override fun sendAllUpdatedArvioinninTilat() {
        logger.debug("sendAllUpdatedArvioinninTilat called but no client configured, skipping.")
    }

    @WithSpan
    override fun sendArvioinninTila(suoritus: YkiSuoritusEntity) {
        logger.debug("sendArvioinninTila called but no client configured, skipping.")
    }
}
