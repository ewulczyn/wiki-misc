DROP TABLE mc;

CREATE TABLE `mc` (
  `curr` varchar(256) DEFAULT NULL,
  `prev` varchar(256) DEFAULT NULL,
  `n` bigint(20) DEFAULT NULL,
   PRIMARY KEY (curr, next)
);

ALTER TABLE `mc` ADD INDEX `curr` (`curr`);
ALTER TABLE `mc` ADD INDEX `prev` (`prev`);