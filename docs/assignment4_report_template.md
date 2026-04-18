# group_name_final_report

## Cover Page

- Course: CS 432 Databases (Track 2 / Assignment 4)
- Institute: IIT Gandhinagar
- Instructor: Dr. Yogesh K. Meena
- Deadline: 18 April 2026, 6:00 PM
- GitHub repository link: <add link>
- Demo video link: <add link>

## 1. Dashboard Enhancement

Describe:
- active session display
- logical entity listing and inspection
- logical object field/value visualization
- logical query result monitoring
- query history tracking
- how backend details are intentionally hidden

Screenshots:
- home
- entities
- entity details
- query monitor
- query history

## 2. Performance Evaluation Experiments

### 2.1 Experiment Design

Document workload, dataset, run count, and whether execute mode was used.

### 2.2 Measured Metrics

- average query latency
- throughput (ops/sec)
- ingestion latency
- metadata lookup overhead
- transaction coordination overhead
- distribution across SQL/Mongo/Buffer

### 2.3 Results Table

| Metric | Average | P50 | P95 | Throughput |
|---|---:|---:|---:|---:|
| Ingestion |  |  |  |  |
| Logical Query |  |  |  |  |
| Metadata Lookup |  |  |  |  |
| Tx Coordination Overhead |  |  |  |  |

## 3. Comparative Analysis

### 3.1 Scenarios

- retrieving user records through the logical query interface vs direct SQL queries
- logical nested data retrieval vs direct Mongo retrieval
- logical update across entities vs direct backend updates

### 3.2 Visualizations

Include:
- latency bar chart
- throughput line chart
- summary table

### 3.3 Interpretation

Discuss where abstraction adds overhead and where it improves developer productivity and data accessibility.

## 4. Final Packaging

Summarize reproducibility:
- dependency installation
- backend configuration
- ingestion API run command
- logical query interface run command
- dashboard launch command

## 5. System Limitations

Document technical constraints and operational assumptions.

## 6. Conclusion

Summarize final system completeness, scalability potential, and lessons learned.
