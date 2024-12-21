#here we will map url to view functions.
from django.contrib import admin
from django.urls import path

from.import views

urlpatterns =[
    path('',views.home,name='home'),
    path('output',views.output,name='output'),
    path('uploadfile/',views.uploadfile,name="uploadfile" ),

]