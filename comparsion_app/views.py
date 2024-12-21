from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse
from .models import PredefinedFile
import pandas as pd
from comparsion_app.models import uploadfile

# Create your views here.
#request-handler(request --> response)

#using this veiws we can do following things
#1) we can pull a data from database
def home(request):
    return render(request,'index.html')

def output(request):
    return render(request,'results.html')


def uploadfile(request):

    if(request.method=="post"):
        file= request.POST.get("file")

        en=uploadfile(file=file)
        en.save()

    return render(request,'results.html')



