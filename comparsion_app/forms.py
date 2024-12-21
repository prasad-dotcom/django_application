from django import forms

class UserFileForm(forms.Form):
    file = forms.FileField(label="upload your excel files")

class FileUploadForm(forms.ModelForm):
    class Meta:
        model =  
        fields =['file']    