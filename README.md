# SGDC

## Introduction

SGDC is a solution to score-guided denial constraint discovery.  
Given a relational dataset, SGDC efficiently discovers high-quality denial constraints (DCs) whose scores exceed a user-specified threshold, instead of enumerating all minimal valid DCs.

Our method integrates three components in an iterative framework:

- sample-based discovery,
- score-driven selective validation,
- sample augmentation using DC-violating tuple pairs.

To improve efficiency, SGDC adopts a score approximation strategy with a provable high-probability error bound, enabling substantially faster discovery with only minor quality degradation.

## Requirements

- Java 11 or later
- Maven 3.1.0 or later

## Usage

After building the project with Maven, you can obtain `SGDC.jar`.

For example, you can run:

```shell
java -jar SGDC.jar ./data/airport.csv
