# Generated by Django 5.0.6 on 2024-06-13 14:26

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("django_async_job_pipelines", "0002_alter_jobdbmodel_errors"),
    ]

    operations = [
        migrations.RenameField(
            model_name="jobdbmodel",
            old_name="errors",
            new_name="error",
        ),
    ]
