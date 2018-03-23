from collections import namedtuple

person_ethno_profile = namedtuple('person_ethno_profile', 'visible_minority, first_nations, metis, inuit, NA, undisclosed')
self_identifies_profile = namedtuple('self_identifies_profile', 'disabled, less_than_ten, other, NA, undisclosed')
visit_tuple_structure = namedtuple('visit_tuple_structure', 'date, main_applicant_id, visit_object')
a_person = namedtuple('a_person', 'ID, Lname, Fname, DOB, Age, Gender, Ethnicity, SelfIdent')