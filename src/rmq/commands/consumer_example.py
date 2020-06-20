from sqlalchemy.dialects.mysql import insert
from database.models.search import SearchEngineQuery
from rmq.utils import TaskStatusCodes
from rmq.commands import Consumer


class ConsumerExample(Consumer):
    def build_message_store_stmt(self, message_body):
        message_body['status'] = TaskStatusCodes.SUCCESS.value
        del message_body['created_at']
        del message_body['updated_at']
        stmt = insert(SearchEngineQuery)
        stmt = stmt.on_duplicate_key_update({
            'status': stmt.inserted.status
        }).values(message_body)
        return stmt
