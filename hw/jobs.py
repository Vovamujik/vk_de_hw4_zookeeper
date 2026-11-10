"""Задачи MapReduce варианта Go: частоты навыков и медианы зарплат по комбинациям навыков."""

import operator
import statistics

from hw import normalize as N
from hw.mapreduce import Job


def map_skill_combinations(fields: list[str], cols: dict[str, int]):
    """Для каждой Go-вакансии: (навык,), (навык, навык), (навык, навык, навык) -> 1."""
    if N.is_go_title(fields[cols["name"]]):
        for combo in N.skill_combinations(N.vacancy_skills(fields[cols["key_skills"]])):
            yield combo, 1


def reduce_sum(key: tuple, values: list[int]) -> int:
    return sum(values)


def map_salary_by_combination(fields: list[str], cols: dict[str, int]):
    """Комбинация навыков -> (предложение, зарплата); пустой кортеж — все Go-вакансии с зарплатой.

    Предложение — работодатель, должность, навыки и вилка: одна вакансия, размещённая
    в нескольких городах, даёт несколько записей, но в медиану должна попасть один раз.
    """
    if not N.is_go_title(fields[cols["name"]]):
        return
    salary = N.salary_net_rub(
        fields[cols["salary_from"]], fields[cols["salary_to"]], fields[cols["currency"]], fields[cols["gross"]]
    )
    if salary is None:
        return
    offer = tuple(fields[cols[c]] for c in N.OFFER_FIELDS)
    for combo in N.skill_combinations(N.vacancy_skills(fields[cols["key_skills"]]), k_min=0):
        yield combo, (offer, salary)


def reduce_offer_median(key: tuple, values: list[tuple]) -> tuple[int, int, float]:
    """(уникальных предложений, разных работодателей, медиана зарплаты по предложениям)."""
    offers = dict(values)  # копии одного предложения схлопываются, зарплата у них одна
    employers = {offer[0] for offer in offers if offer[0]}  # пустой id, как NULL в count(DISTINCT), не считаем
    return len(offers), len(employers), statistics.median(offers.values())


def map_control(fields: list[str], cols: dict[str, int]):
    """Контрольные числа для сверки с DuckDB: вакансий Go, из них с навыками, всего упоминаний навыков."""
    if N.is_go_title(fields[cols["name"]]):
        skills = N.vacancy_skills(fields[cols["key_skills"]])
        yield ("go_vacancies",), 1
        if skills:
            yield ("with_skills",), 1
            yield ("skill_mentions",), len(skills)


SKILL_COUNTS = Job(
    "skill_counts", map_skill_combinations, reduce_sum, combiner=operator.add, prefilter=N.go_candidate_lines
)
# Медиана не раскладывается на частичные итоги, поэтому combiner нет: в reduce уходят все значения.
SALARY_BY_COMBINATION = Job(
    "salary_by_combination", map_salary_by_combination, reduce_offer_median, prefilter=N.go_candidate_lines
)
CONTROL = Job("control", map_control, reduce_sum, combiner=operator.add, prefilter=N.go_candidate_lines)
JOBS = {"skills": SKILL_COUNTS, "salary": SALARY_BY_COMBINATION, "control": CONTROL}
