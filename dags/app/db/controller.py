from collections import Counter
from logging import getLogger
from typing import Dict, List

from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import base

from app.db.models import Base, Company, CompanyDirectory, Skill, TopSkill, TypeOfBusiness, Vacancy
from app.settings import THRESHOLD
from app.utils import normalize
# from utils import get_logger


class Driver:
    """
    """

    def __init__(self, engine: base.Engine, verbose: bool) -> None:
        self.engine = engine
        self.session_factory = sessionmaker(self.engine, expire_on_commit=True)
        self._data_list = []
        self.logger = getLogger(__name__)

    def create_db(self) -> None:
        return self.db_execute(
            lambda: Base.metadata.create_all(self.engine)
        )

    def destroy_db(self) -> None:
        return self.db_execute(
            lambda: Base.metadata.drop_all(self.engine)
        )
        
    def db_execute(self, action) -> bool:
        try:
            action()
            return True
        
        except Exception as e:
            return False

    def add_data(self, data: List[Base], commit=True) -> bool:
        if commit:
            return self._add_record(data)
        else:
            self._data_list.append(*data)
            if len(self._data_list) >= THRESHOLD:
                return self.commit()

            return True

    def _add_record(self, data: List[Base]) -> bool:
        try:
            with self.session_factory() as session:
                session.add_all(data)
                session.commit()

            return True
        except SQLAlchemyError as e:
            self.logger.error(e)
            return False

    def commit(self) -> bool:
        data = [*self._data_list]
        self._data_list = []
        return self._add_record(data)


class Controller(Driver):
    """
    """

    def add_company(
            self,
            data: List[Dict[str, str]],
            commit=True,
            ) -> bool:
        return self.add_data(list(map(
            lambda x: Company(**x), data)), commit=commit,
            )

    def add_vacancy(
            self,
            data: List[Dict[str, str]],
            commit=True,
            ) -> bool:
        return self.add_data([
            Vacancy(
                company_name=item['company_name'],
                position=item['position'],
                job_description=item['job_description'],
                key_skills=[
                    Skill(
                        name=skill_item['name'],
                    ) for skill_item in item['key_skills']
                ],
            ) for item in data
        ], commit=commit,
            )

    def add_skill(
            self,
            data: List[Dict[str, str]],
            commit=True,
            ) -> bool:
        return self.add_data(list(map(
            lambda x: Skill(**x), data)), commit=commit,
            )
        
    def add_type_of_business(
            self,
            data: List[Dict[str, str]],
            commit=True) -> bool:
        return self.add_data(list(map(
            lambda x: TypeOfBusiness(**x), data)), commit=commit,
            )

    def add_company_dir(self,
                    data: List[Dict[str, str]],
                    commit=True) -> bool:
        return self.add_data(list(map(
            lambda x: CompanyDirectory(**x), data)), commit=commit,
            )
        
    def calculate_top_skills(self):
        skill_counter = Counter()
        
        with self.session_factory() as session:
            
            telecom_company_list = list(map(lambda x: normalize(x[0]), session.execute(select(CompanyDirectory.name))))
            
            for vacancy in session.execute(select(Vacancy)):
                if vacancy[0].company_name.lower() in telecom_company_list:
                    for skill in vacancy[0].key_skills:
                        skill_counter[skill.name] += 1
                    
            session.add_all([
                TopSkill(name=skill_name, count=skill_count) 
                for skill_name, skill_count in skill_counter.most_common(10)
                ])
        
        session.commit()
