#Local variables
$SQLServer = "YOUR SQL INSTANCE"
$SQLDBName = "DataLakeStoreInfo"
$ADLStoreAccountName = "YOUR STORE ACCOUNT NAME"
$SubscriptionId = "YOUR AZURE SUBSCRIPTION ID"

function Get-DataLakeStoreChildItemRecursive ([hashtable] $Params) {
    $AllFiles = New-Object Collections.Generic.List[Microsoft.Azure.Commands.DataLakeStore.Models.DataLakeStoreItem];
    recurseDataLakeStoreChildItem -AllFiles $AllFiles -Params $Params
    $AllFiles
}
function recurseDataLakeStoreChildItem ([System.Collections.ICollection] $AllFiles, [hashtable] $Params) {
    $ChildItems = Get-AzureRmDataLakeStoreChildItem @Params;
    $Path = $Params["Path"];
    foreach ($ChildItem in $ChildItems) {
        switch ($ChildItem.Type) {
            "FILE" {
                $AllFiles.Add($ChildItem);
            }
            "DIRECTORY" {
                $Params.Remove("Path");
                $Params.Add("Path", $Path + "/" + $ChildItem.Name);
                recurseDataLakeStoreChildItem -AllFiles $AllFiles -Params $Params;
            }
        }
    }
}
#https://stackoverflow.com/questions/41274852/powershell-recursive-in-azure-data-lake-store?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa

Login-AzureRmAccount | Out-Null
Set-AzureRmContext -SubscriptionId $SubscriptionId | Out-Null
Register-AzureRmResourceProvider -ProviderNamespace "Microsoft.DataLakeStore" | Out-Null

$Files = Get-DataLakeStoreChildItemRecursive `
    -Params @{ 'Path' = '/'; 'Account' = $ADLStoreAccountName } `
    | Select-Object -Property Name, Length, Path, LastWriteTime `
    | ForEach-Object -Process {$_.Length = ($_.Length)/1024/1024; $_}


$SQLConnection = New-Object System.Data.SqlClient.SqlConnection
$SQLConnection.ConnectionString = "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"

ForEach ($File in $Files)
    {    
    $SQLCmd = New-Object System.Data.SqlClient.SqlCommand
    $SQLCmd.Connection = $SQLConnection

    #Open connection and target procedure
    $SQLConnection.Open()
    $SQLCmd.CommandType = [System.Data.CommandType]::StoredProcedure
    $SQLCmd.CommandText = "[dbo].[InsertADLStoreRecord]"
        
    #Add procedure params
    $SQLCmd.Parameters.Add("AccountName",$ADLStoreAccountName) | out-Null
    $SQLCmd.Parameters.Add("@FileName",$File.Name) | out-Null
    $SQLCmd.Parameters.Add("@FullPath",$File.Path) | out-Null
    $SQLCmd.Parameters.Add("@Size",$File.Length) | out-Null
    $SQLCmd.Parameters.Add("@ModifiedDate",$File.LastWriteTime) | out-Null
        
    #Execute procedure and close connection
    $Result = $SQLCmd.ExecuteNonQuery()
    $SQLConnection.Close()
    }


