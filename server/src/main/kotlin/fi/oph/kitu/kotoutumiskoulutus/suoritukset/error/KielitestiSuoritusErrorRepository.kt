package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.stereotype.Repository

@Repository
interface KielitestiSuoritusErrorRepository :
    CrudRepository<KielitestiSuoritusError, Int>,
    PagingAndSortingRepository<KielitestiSuoritusError, Int> {
    fun findAllByCompleted(completed: Boolean): List<KielitestiSuoritusError>
}
