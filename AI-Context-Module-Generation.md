# AI Context: Guide Generation Process

**Target Goal:** Create a continuous, comprehensive, book-like report based on data engineering learning modules regarding Databricks, Lakehouse, and Lakeflow.

## Current Progress
*   **Introduction Elements:** Completed (Apache Spark, Databricks analogy, ETL, Delta Lake, Medallion Architecture).
*   **Module 1 (Data Ingestion with Lakeflow Connect):** Completed. 

## The Required "Book" Style
For every upcoming module prompted by the user, the AI must strictly follow this structure:
1.  **Layman's terms first:** Give an easy-to-understand, conceptual explanation (resembling an interesting tech book). Use analogies (like pipelines, factories, etc.)
2.  **Flowchart / Image:** Use Mermaid.js to create a visual diagram OR reference an image from the `assets` folder (e.g., `![alt text](assets/image.png)`).
3.  **Detailed Overview & Technical Syntax:** Dive down into the specific components, Databricks methods, and provide code blocks (SQL/PySpark) handling the actual configurations and implementations detailed in the user's prompt.

## Pending Modules (To be provided by user later)
*   **Module 2:** Deploy Workloads with Lakeflow Jobs (Topics: Task building blocks, UI creation, Task Config, Schedules, Triggers, Job Monitoring, dynamic workflows).
*   **Module 3:** Build Data Pipelines with Lakeflow Spark Declarative Pipelines (Topics: Datasets, Pipeline settings, Data Quality Expectations, Streaming Joins, CDC with AUTO CDC INTO, Deployments).
*   **Module 4:** DevOps Essentials for Data Engineering (Topics: SWE Best Practices, Modularizing PySpark, CI/CD, PySpark Unit/Integration testing, Git Version Control, Asset deployments).

## AI Instructions for Next Prompt
1. Wait for the user to provide the detailed context for Module 2.
2. Read the prompt and cross-reference with this context file to understand the stylistic constraints.
3. Append Module 2 to the existing `Complete-Data-Engineering-Guide.md` using the exact same formatting:
    * `## Module 2: ...`
    * Layman / Conceptual Introduction.
    * Mermaid flowcharts illustrating Jobs/Orchestration.
    * Deep dive into tasks, schedules, dependencies, idempotency, UI configs, and syntax where applicable.
4. Keep the output clean, accessible, and structured identically across modules. Update this context document's "Current Progress" section after each step.