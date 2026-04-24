from redis_queue import Consumer
from redis import Redis
from neo4j_birtix_db_repo.repos import CalendarSectionRepository
from neo4j_birtix_db_repo.models import CalendarSectionPayload
from neo4j import GraphDatabase

URI = "bolt://mirror-db:7687"
AUTH = ("neo4j", "password")
redis = Redis('queue', 6379, decode_responses=True)
driver = GraphDatabase.driver(URI, auth=AUTH)


class CalendarConsumer(Consumer):
    def __init__(self, calendar_sections_repo: CalendarSectionRepository, redis, stream_key, group_name, consumer, buffer_size, fulsh_time):
        super().__init__(redis, stream_key, group_name, consumer, buffer_size, fulsh_time)
        self.repo = calendar_sections_repo

    def handle_batch(self, batch):
        print(batch)
        self.repo.upsert_batch([CalendarSectionPayload(**calendar_sections_tuple[1]) for calendar_sections_tuple in batch])


calendar_sections_repo = CalendarSectionRepository(driver)

calendar_sections_consumer = CalendarConsumer(calendar_sections_repo, redis, "calendar_sections", "group", "calendar_sections", 1000, 1)

calendar_sections_consumer.run()
