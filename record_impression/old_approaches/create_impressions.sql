DROP TABLE IMP;

CREATE TABLE `IMP` (
  `anonymous` varchar(5) DEFAULT NULL,
  `banner` varchar(128) DEFAULT NULL,
  `campaign` varchar(128) DEFAULT NULL,
  `country` varchar(32) DEFAULT NULL,
  `device` varchar(8) DEFAULT NULL,
  `dt` char(19) DEFAULT NULL,
  `project` varchar(16) DEFAULT NULL,
  `reason` varchar(32) DEFAULT NULL,
  `result` char(4) DEFAULT NULL,
  `uselang` varchar(16) DEFAULT NULL,
  `db` varchar(32) DEFAULT NULL,
  `count` bigint(20) DEFAULT NULL,
  `bucket` int DEFAULT NULL,
  `spider` boolean DEFAULT 0,
   PRIMARY KEY (anonymous, banner, bucket, campaign, country, db, device, dt, project, reason, result, uselang, spider)
);

ALTER TABLE `IMP` ADD INDEX `banner` (`banner`);
ALTER TABLE `IMP` ADD INDEX `dt` (`dt`);
ALTER TABLE `IMP` ADD INDEX `reason` (`reason`);
ALTER TABLE `IMP` ADD INDEX `campaign` (`campaign`);
