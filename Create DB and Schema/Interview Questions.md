
### Q1: what is wrong: create file format if not exists stage_sch.csv_file_format type = 'csv', compression = 'auto', field_delimeter = ',', record_delimeter = '\n', skip_header = 1, field_optionally_enclosed_by = '\042', null_if = ('\N');

**Using Multiple Schemas in a Single Database**
This is great when:
- Your logical layers (like staging, raw, curated, analytics) are closely related.
- You want to simplify access control by managing users at the database level and granting different schema-level permissions.
- It’s important to minimize data duplication and enable easier cross-schema joins.
Schemas are lightweight, so performance and storage impact is minimal. They’re a clean way to organize data layers while maintaining tight cohesion in one place.
**Using Multiple Databases**
Choose this when:
- Your data layers represent completely isolated domains with different lifecycles—e.g., finance vs. HR vs. marketing.
- You want to enforce stricter separation for compliance, billing, or team autonomy.
- You're working in an environment like Snowflake, where databases can belong to different compute and storage strategies.
This approach supports clean data governance boundaries and makes it easier to manage things like backups or replication independently.

**If I were architecting a layered data warehouse—say with staging → transformation → presentation—I’d usually recommend schemas within a single database. It keeps data lineage clear, reduces complexity, and makes cross-layer transformations much smoother. Unless there's a strong organizational or technical reason to split into databases, schemas strike a good balance between structure and flexibility.**


### Q2: If you have common objects like UDF's or tag objects that can be shared across multiple layers, would you create a separate schema for them? why or why not?

Yes—use a separate schema for shared objects like UDFs and tags. It keeps things clean, reusable, and easier to manage across layers.

✅ Why Create a Separate Schema for Shared Objects
- **Encapsulation of Utilities**
Keeping shared logic in its own schema (like util, common, or shared_assets) helps clearly distinguish business logic from reusable infrastructure code.
- **Simplifies Permissions & Governance**
You can grant usage permissions selectively—teams can access the reusable objects without being exposed to full access on your core data schemas.
- **Promotes Reusability and DRY Principles**
Centralized UDFs and tags eliminate redundancy. Teams can reference shared definitions and updates propagate uniformly—less maintenance, fewer bugs.
- **Clean Dependency Management**
It makes dependencies explicit. You’re not wondering which layer defines what. A shared schema signals a clear purpose.

### Q3: When setting up a database for development activities in Snowflake, would you choose to create a transient database? If yes, why?

Yes—a transient database is a smart choice for dev environments in Snowflake because it doesn’t retain fail-safe data, making it more cost-efficient. Since development work is usually temporary or iterative, you typically don’t need long-term recovery features like fail-safe. It’s ideal for quick testing, prototyping, and ETL experimentation without bloating storage costs.
Just don’t forget to back up anything worth keeping—transient really means transient!

### Copy command is highly efficient as Snowflake automatically maintains metadata, history for all data loads.

