from django.db import models

# Create your models here.

class PredefinedFile(models.Model):
    name=models.CharField(max_length=255)
    file = models.FileField(upload_to="predefined_files")
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
    

class uploadfile(models.Model):
    file = models.FileField(max_length=255)

