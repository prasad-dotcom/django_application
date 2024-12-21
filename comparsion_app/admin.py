from django.contrib import admin
from .models import PredefinedFile

# Register your models here.
admin.site.register(PredefinedFile)


class PredefinedFileAdmin(admin.ModelAdmin):
    list_display =('name' , 'file')

class uploadfiles(admin.ModelAdmin):
    list_display =('file', 'uploaded_at' )    
