function SetEnvironmentVariable([string] $name, [string] $value)
{
	if($value -eq "")
	{
		Write-Host "Enter value for " $name
		$value = Read-Host
	}
    Write-Host $name $value
    [Environment]::SetEnvironmentVariable($name, $value, [EnvironmentVariableTarget]::Machine)    
}
SetEnvironmentVariable "CONFLUENT_KAFKA_WORKSHOP_BOOTSTRAP_SERVERS"