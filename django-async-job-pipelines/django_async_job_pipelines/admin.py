from django.contrib import admin

from .models import JobDBModel, PipelineDBModel, PipelineJobsDBModel


class JobAdmin(admin.ModelAdmin):
    list_display = ["id", "name", "status", "date_updated", "error", "previous_job"]
    list_filter = ["status", "name"]
    readonly_fields = ["previous_job"]

    @admin.action(description="Mark as NEW")
    def mark_as_new(self, request, queryset):
        queryset.update(status=JobDBModel.JobStatus.NEW)


class PipelineAdmin(admin.ModelAdmin):
    pass


class PiplineJobsAdmin(admin.ModelAdmin):
    pass


admin.site.register(JobDBModel, JobAdmin)
admin.site.register(PipelineDBModel, PipelineAdmin)
admin.site.register(PipelineJobsDBModel, PiplineJobsAdmin)
