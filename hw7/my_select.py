from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    # Найти 5 студентов с наибольшим средним баллом по всем предметам.
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result

def select_2(discipline_id):
    # Найти студента с наивысшим средним баллом по определенному предмету.
    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Discipline).filter(Discipline.id == discipline_id).group_by(Student.id, Discipline.name).order_by(desc('average_grade')).limit(1).all()
    return result

def select_3(discipline_id):
    # Найти средний балл в группах по определенному предмету.
    result = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Discipline).join(Group).filter(Discipline.id == discipline_id).group_by(Group.name, Discipline.name).order_by(desc('average_grade')).all()
    return result

def select_4():
    # Найти средний балл на потоке (по всей таблице оценок).
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).all()
    return result

def select_5(teacher_id):
    # Найти какие курсы читает определенный преподаватель.
    result = session.query(Teacher.fullname, Discipline.name) \
        .select_from(Discipline).join(Teacher).filter(Teacher.id == teacher_id).order_by(Discipline.name).all()
    return result

def select_6(group_id):
    # Найти список студентов в определенной группе.
    result = session.query(Group.name, Student.fullname) \
        .select_from(Student).join(Group).filter(Group.id == group_id).order_by(Student.fullname).all()
    return result

def select_7(group_id, discipline_id):
    # Найти оценки студентов в отдельной группе по определенному предмету.
    result = session.query(Group.name, Discipline.name, Student.fullname, Grade.grade, Grade.date_of) \
        .select_from(Grade).join(Student).join(Group).join(Discipline).filter(and_(Group.id == group_id, Discipline.id == discipline_id)).order_by(Student.fullname, Grade.date_of).all()
    return result

def select_8(teacher_id):
    # Найти средний балл, который ставит определенный преподаватель по своим предметам.
    result = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Teacher).join(Discipline).join(Grade).filter(Teacher.id == teacher_id).group_by(Discipline.name, Teacher.fullname).order_by(Discipline.name).all()
    return result

def select_9(student_id):
    # Найти список курсов, которые посещает определенный студент.
    result = session.query(Student.fullname, Discipline.name) \
        .select_from(Discipline).join(Grade).join(Student).filter(Student.id == student_id).group_by(Discipline.name, Student.fullname).order_by(Discipline.name).all()
    return result

def select_10(teacher_id, student_id):
    # Список курсов, которые определенному студенту читает определенный преподаватель.
    result = session.query(Teacher.fullname, Student.fullname, Discipline.name) \
        .select_from(Discipline).join(Teacher).join(Grade).join(Student).filter(and_(Teacher.id == teacher_id, Student.id == student_id)).group_by(Discipline.name, Teacher.fullname, Student.fullname).order_by(Discipline.name).all()
    return result

def select_11(teacher_id, student_id):
    # Средний балл, который определенный преподаватель ставит определенному студенту.
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Discipline).join(Teacher).join(Student).filter(and_(Teacher.id == teacher_id, Student.id == student_id)).group_by(Teacher.fullname, Student.fullname).all()
    return result

def select_12(group_id, discipline_id):
    # Оценки студентов в определенной группе по определенному предмету на последнем занятии.
    subquery = (select(func.max(Grade.date_of)).join(Student).join(Group).filter(and_(Group.id == group_id, Grade.discipline_id == discipline_id)).scalar_subquery())
    result = session.query(Group.name, Student.fullname, Discipline.name, Grade.grade, Grade.date_of) \
        .select_from(Grade).join(Discipline).join(Student).join(Group).filter(and_(Group.id == group_id, Discipline.id == discipline_id, Grade.date_of == subquery)).order_by(Student.fullname).all()
    return result


if __name__ == '__main__':
    print(select_1())
    print(select_2(1))
    print(select_3(1))
    print(select_4())
    print(select_5(1))
    print(select_6(1))
    print(select_7(1, 1))
    print(select_8(1))
    print(select_9(1))
    print(select_10(1, 1))
    print(select_11(1, 1))
    print(select_12(1, 1))