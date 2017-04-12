/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:30:36
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for city_table
-- ----------------------------
DROP TABLE IF EXISTS `city_table`;
CREATE TABLE `city_table` (
  `city_id` varchar(255) NOT NULL,
  `city_name` varchar(255) NOT NULL,
  PRIMARY KEY (`city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
