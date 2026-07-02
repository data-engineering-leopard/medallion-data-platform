from setuptools import setup, find_packages

setup(
    name="my_project",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["pyspark>=3.5.0", "pyyaml>=6.0"],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "bronze_online_tcg=my_project.tasks.bronze.bronze_online_tcg:main",
            "bronze_salesforce=my_project.tasks.bronze.bronze_salesforce:main",
            "silver_task=my_project.tasks.silver.silver_task:main",
            "dim_customers=my_project.tasks.gold.dim_customers:main",
            "fact_orders=my_project.tasks.gold.fact_orders:main",
            "dim_leads=my_project.tasks.gold.dim_leads:main",
        ]
    },
)
