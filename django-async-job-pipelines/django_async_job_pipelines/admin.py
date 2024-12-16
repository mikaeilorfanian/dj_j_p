from django.contrib import admin

from .models import JobDBModel, PipelineDBModel, PipelineJobsDBModel


class JobAdmin(admin.ModelAdmin):
    pass


class PipelineAdmin(admin.ModelAdmin):
    pass


class PiplineJobsAdmin(admin.ModelAdmin):
    pass


admin.site.register(JobDBModel, JobAdmin)
admin.site.register(PipelineDBModel, PipelineAdmin)
admin.site.register(PipelineJobsDBModel, PiplineJobsAdmin)
