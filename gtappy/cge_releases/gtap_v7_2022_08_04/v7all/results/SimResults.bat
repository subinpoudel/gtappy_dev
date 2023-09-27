
::  =================================
::  File Setting: May require changes  
::  =================================

::  -------------------------------
:: Step 1: Project name and folders
::  -------------------------------

SET DATDIR=..
SET SIMDIR=..\savesims

SET RESDIR=.
SET RESOUT=..\res
SET RESWRK=..\wrk

::  copy GTAPView to results directory
copy %DATDIR%\baseview.har .\wrk\gtapview.har


::  -------------------------------
::  Step 2: Define simulation names
::  -------------------------------
 
SET Sim01=agpr20
SET Sim02=rtms5
SET Sim03=beef50

::  ----------------------------
::  DO NOT EDIT BEYOND THIS LINE
::  ----------------------------

SET MAPRES=%RESDIR%\results.map

::  --------------------------
::  Step 3: Copy Results files
::  --------------------------
echo converting results. . .

sltoht -map=%MAPRES% %SIMDIR%\%Sim01% %RESDIR%\in\%Sim01%.sol
     if errorlevel 1 goto error

sltoht -map=%MAPRES% %SIMDIR%\%Sim02% %RESDIR%\in\%Sim02%.sol
     if errorlevel 1 goto error

sltoht -map=%MAPRES% %SIMDIR%\%Sim03% %RESDIR%\in\%Sim03%.sol
     if errorlevel 1 goto error

::  -----------------------
::  Step 4: Combine results
::  -----------------------

echo combine results. . .
cd %RESdir%\src

 cmbhar -sti cmbres.sti

cd..

::  -----------------------
::  Step 5: Results summary
::  -----------------------

echo create results summary file. . .

cd %RESDIR%\src

tablo -pgs Allres
     if errorlevel 1 goto error

gemsim -cmf Allres.cmf -p1=..\%DATdir% -p2=%RESWRK% -p3=.. -p4=%RESout%
     if errorlevel 1 goto error

cd..

::  -----------------------
::  Step 6: Results in csv
::  -----------------------
cd %RESDIR%\res

::  6A: Global results 
::  ------------------
 har2csv allres.har glob.csv glob

::  6B: Regional Macro results 
::  --------------------------
:: 6B.1 Full dimension 
 har2csv allres.har rmac.csv rmac
:: 6B.2 Aggregated results
 har2csv allres.har rmca.csv rmca

::  6C: Regional Commodity results 
::  ------------------------------
:: 6C.1 Full dimension 
 har2csv allres.har rcom.csv rcom
:: 6C.2 Aggregated results
 har2csv allres.har rcma.csv rcma

::  6D: Regional Production results 
::  -------------------------------
:: 6D.1 Full dimension 
 har2csv allres.har ract.csv ract
:: 6D.2 Aggregated results
 har2csv allres.har raca.csv raca

::  6E: Bilateral Trade results 
::  ---------------------------
:: 6E.1 Full dimension 
 har2csv allres.har trad.csv trad
:: 6E.2 Aggregated results
 har2csv allres.har trda.csv trda

::  6F: Endowment supply results 
::  ----------------------------
:: 6E.1 Full dimension 
 har2csv allres.har ends.csv ends
:: 6E.2 Aggregated results
 har2csv allres.har enda.csv enda

::  6G: Detailed Factor results 
::  ---------------------------
:: 6G.1 Full dimension 
 har2csv allres.har facs.csv facs
:: 6G.2 Aggregated results
 har2csv allres.har faca.csv faca

cd..

::  =================
::  Terminal messages
::  =================
echo Results processing completed !
goto endOK          

:error           
echo PROBLEM !!!! examine most recent Log
exit /b 1            
:endOK     

::====
:: END
::====
:skip