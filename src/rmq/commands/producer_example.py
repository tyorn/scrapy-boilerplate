from sqlalchemy import select, update
from database.models.search import SearchEngineQuery
from rmq.utils import TaskStatusCodes
from rmq.commands import Producer


class ProducerExample(Producer):
    def build_task_query_stmt(self, chunk_size):
        stmt = select([SearchEngineQuery]).where(
            SearchEngineQuery.status == TaskStatusCodes.NOT_PROCESSED.value,
        ).order_by(SearchEngineQuery.id.asc()).limit(chunk_size)
        return stmt

    def build_task_update_stmt(self, db_task, status):
        return update(SearchEngineQuery).where(SearchEngineQuery.id == db_task['id']).values({'status': status})
