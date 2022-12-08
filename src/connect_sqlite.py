import sqlite3
import pandas as pd

# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory
db_connect = sqlite3.connect('projPt3.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

# String variable for passing queries to cursor
clinic_query = """
    CREATE TABLE IF NOT EXISTS Clinic (
    clinicNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(30),
    address VARCHAR(30) UNIQUE,
    managerNo VARCHAR(10) NOT NULL,
    phoneNo INT NOT NULL UNIQUE,
    FOREIGN KEY (managerNo) REFERENCES Staff(staffNo)
    );
    """
staff_query = """
    CREATE TABLE IF NOT EXISTS Staff (
    staffNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(30) NOT NULL,
    address VARCHAR(30) NOT NULL,
    phoneNo INT NOT NULL UNIQUE,
    DOB TEXT,
    position VARCHAR(30) NOT NULL,
    salary INT,
    clinicNo VARCHAR(10) NOT NULL,
    FOREIGN KEY (clinicNo) REFERENCES Clinic
    );
    """
owner_query = """
    CREATE TABLE IF NOT EXISTS Owner(
    ownerNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(30),
    phoneNo INT,
    address VARCHAR(30) NOT NULL
    );
    """
pet_query = """
    CREATE TABLE IF NOT EXISTS Pet (
    petNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(30) NOT NULL,
    species VARCHAR(30) NOT NULL,
    breed VARCHAR(30) NOT NULL,
    DOB TEXT,
    color VARCHAR(10),
    ownerNo VARCHAR(10) NOT NULL,
    clinicNo VARCHAR(10) NOT NULL,
    FOREIGN KEY (ownerNo) REFERENCES Owner,
    FOREIGN KEY (clinicNo) REFERENCES Clinic
    );
    """
exam_query = """
    CREATE TABLE IF NOT EXISTS Examination (
    examNo VARCHAR(10) NOT NULL PRIMARY KEY,
    chiefComplaint VARCHAR(100) NOT NULL,
    description VARCHAR(100),
    dateSeen TEXT NOT NULL,
    actions VARCHAR(100),
    staffNo VARCHAR(10) NOT NULL,
    petNo VARCHAR(10) NOT NULL,
    FOREIGN KEY (staffNo) REFERENCES Staff,
    FOREIGN KEY (petNo) REFERENCES Pet
    );
    """

# Execute querys, the result is stored in cursor
cursor.execute(clinic_query)
cursor.execute(staff_query)
cursor.execute(owner_query)
cursor.execute(pet_query)
cursor.execute(exam_query)

# Insert rows into table
clinic_insert = """
    INSERT OR IGNORE INTO Clinic 
    VALUES 
        ('C1', 'Westpark Clinic', '8110 SW 70th St', 'S03', 8001214523),
        ('C2', 'Eastpark Clinic', '7250 E 6th St', 'S07', 8003432001),
        ('C3', 'Northpark Clinic', '133 NW 18th Ave', 'S02', 8002223149),
        ('C4', 'Southpark Clinic', '7661 Blossom Rd', 'S09', 8009887329),
        ('C5', 'Cleveland Pet Clinic', '2351 S 18th Blvd', 'S11', 8001182560);
    """
staff_insert = """
    INSERT OR IGNORE INTO Staff 
    VALUES 
        ('S01', 'Lisa Adams', '920 W 15th St', 4401214523, '1992-01-09', 'Secretary', 12000, 'C1'),
        ('S02', 'Matthew Stevens', '4210 Forestwood Ln', 2163432001, '1995-05-11', 'Manager', 25000, 'C3'),
        ('S07', 'Brian Patel', '3667 NW 28th St', 3509887329, '1975-12-13', 'Manager', 25000, 'C2'),
        ('S03', 'Richard Ryans', '117 Westshire Rd', 3052223149, '1981-09-20', 'Manager', 26000, 'C1'),
        ('S09', 'Mat Perry', '782 Grove Blvd', 2111182560, '1988-05-14', 'Manager', 25000, 'C4'),
        ('S11', 'Ashley Smith', '157 58th Ave', 4661662319, '1990-06-29', 'Manager', 28000, 'C5');
    """
owner_insert = """
    INSERT OR IGNORE INTO Owner 
    VALUES 
        ('O01', 'Steven Tucker', 3308771922, '480 Lynn St'),
        ('O02', 'Juliette Sparks', 5609521000, '887 S 13th St'),
        ('O03', 'Raymond Thompson', 4106311290, '921 Berkley Ave'),
        ('O04', 'Sylvia Spence', 4427861221, '3079 Burke Rd'),
        ('O05', 'Chad Pollard', 3055620087, '3756 Blane Blvd');
    """
pet_insert = """
    INSERT OR IGNORE INTO Pet 
    VALUES 
        ('P01', 'Oreo', 'Dog', 'Havanese', '2017-03-28', 'Brown', 'O03', 'C1'),
        ('P02', 'Pepper', 'Bird', 'African Grey', '1998-05-15', 'Grey', 'O03', 'C1'),
        ('P03', 'Paco', 'Dog', 'Bichon Poodle', '2013-05-05', 'White', 'O02', 'C3'),
        ('P04', 'Larry', 'Frog', 'Dart', '2020-11-18', 'Yellow', 'O05', 'C4'),
        ('P05', 'Henry', 'Cat', 'Siamese', '2016-08-11', 'White', 'O04', 'C2');
    """
exam_insert = """
    INSERT OR IGNORE INTO Examination 
    VALUES 
        ('E001', 'General Check Up', 'Monthly appointment', '2022-12-03', 'None', 'S01', 'P01'),
        ('E002', 'Vaccination', 'Received rabies shot', '2022-11-16', 'None', 'S02', 'P01'),
        ('E003', 'Check Up', 'Plucking feathers', '2020-08-19', 'Medicine Prescribed', 'S01', 'P02'),
        ('E004', 'Broken Leg', 'Surgery for broken leg', '2019-06-22', 'Medicine Prescribed', 'S07', 'P05'),
        ('E005', 'General Check Up', 'Monthly appointment', '2022-10-17', 'None', 'S09', 'P04');
    """

cursor.execute(clinic_insert)
cursor.execute(staff_insert)
cursor.execute(owner_insert)
cursor.execute(pet_insert)
cursor.execute(exam_insert)

# Select data
query1 = """
    SELECT *
    FROM Clinic
    """
query2 = """
    SELECT *
    FROM Staff
    """
query3 = """
    SELECT *
    FROM Owner
    """
query4 = """
    SELECT *
    FROM Pet
    """
query5 = """
    SELECT *
    FROM Examination
    """

queries = [query1, query2, query3, query4, query5]
print("--------------------------------------------------------------------------------------------------")
print("DATABASE CONTENTS")

for query in queries:
    cursor.execute(query)
    column_names = [row[0] for row in cursor.description]
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    print("--------------------------------------------------------------------------------------------------")
    print(df)

desc1 = "List the staff name, pet name, and pet's owner name for all exams done."
query1 = """
    SELECT examNo, dateSeen, e.staffNo, s.name AS staff_name, p.name AS pet_name, o.name AS owner_name
    FROM Staff s, Pet p, Examination e, Owner o
    WHERE e.staffNo = s.staffNo AND e.petNo = p.petNo AND p.ownerNo = o.ownerNo
    """
desc2  = "List the clinics and their managers for managers whose salary is more than $25000."
query2 = """
    SELECT c.clinicNo, managerNo, s.name, position, salary
    FROM Clinic c
    JOIN Staff s WHERE c.managerNo = s.staffNo AND s.salary > 25000
    """
desc3 = "List all pets and their owner's name and phone number."
query3 = """
    SELECT petNo, p.name, p.ownerNo, o.name, phoneNo
    FROM Pet p
    JOIN Owner o WHERE o.ownerNo = p.ownerNo
    """
desc4 = "List the number of pets registered at each clinic."
query4 = """
    SELECT c.clinicNo, COUNT(petNo) AS num_pets
    FROM Clinic c, Pet p
    WHERE p.clinicNo = c.clinicNo
    GROUP BY c.clinicNo
    """
desc5 = "List all exams done before 2022."
query5 = """
    SELECT *
    FROM Examination
    WHERE dateSeen < '2022-01-01'
    """

queries = [(query1,desc1), (query2,desc2), (query3,desc3), (query4,desc4), (query5,desc5)]
print("--------------------------------------------------------------------------------------------------")
print("QUERIES")

for (query,desc) in queries:
    cursor.execute(query)
    column_names = [row[0] for row in cursor.description]
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    print("--------------------------------------------------------------------------------------------------")
    print(desc)
    print(df)
print("--------------------------------------------------------------------------------------------------")

# Close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_connect.close()
