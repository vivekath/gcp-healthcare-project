-- ===============================
-- HOSPITAL A - BRONZE EXTERNAL TABLES
-- ===============================

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.departments_ha` (
  DeptID STRING,
  Name STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.encounters_ha` (
  EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DepartmentID STRING,
  EncounterDate INT64,
  ProcedureCode INT64
  EncounterType STRING,
  InsertedDate INT64,
  ModifiedDate INT64
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.patients_ha` (
  PhoneNumber STRING,
  PatientID STRING,
  FirstName STRING,
  LastName STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  ModifiedDate INT64,
  SSN STRING,
  MiddleName STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-a/patients/*.json']
);


CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.providers_ha` (
  NPI INT64,
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-a/providers/*.json']
);


CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.transactions_ha` (
  ModifiedDate INT64	
  InsertDate INT64	
  MedicareID STRING	
  LineOfBusiness STRING	
  ICDCode STRING	
  ClaimID STRING	
  PayorID STRING	
  AmountType STRING	
  Amount FLOAT64	
  VisitType STRING	
  ProcedureCode INT64	
  PaidDate INT64	
  ServiceDate INT64	
  VisitDate INT64	
  ProviderID STRING	
  PaidAmount FLOAT64	
  DeptID STRING	
  EncounterID STRING	
  PatientID STRING	
  MedicaidID STRING	
  TransactionID STRING	
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-a/transactions/*.json']
);

-- ===============================
-- HOSPITAL B - BRONZE EXTERNAL TABLES
-- ===============================

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.departments_hb`
LIKE `quantum-episode-345713.bronze_dataset.departments_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.encounters_hb`
LIKE `quantum-episode-345713.bronze_dataset.encounters_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.patients_hb`
LIKE `quantum-episode-345713.bronze_dataset.patients_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.providers_hb`
LIKE `quantum-episode-345713.bronze_dataset.providers_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `quantum-episode-345713.bronze_dataset.transactions_hb`
LIKE `quantum-episode-345713.bronze_dataset.transactions_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-20122025/landing/hospital-b/transactions/*.json']
);
