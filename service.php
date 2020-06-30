<?php
include "config.php";
include "utils.php";
$dbConn =  connect($db);

//GET
if ($_SERVER['REQUEST_METHOD'] == 'GET')
{
    if (isset($_GET['job_id']))
    {
      
      $sql = $dbConn->prepare("SELECT * FROM priority_queue where job_id=:job_id");
      $sql->bindValue(':job_id', $_GET['job_id']);
      $sql->execute();
      header("HTTP/1.1 200 OK");
	  $result_array= $sql->fetch(PDO::FETCH_ASSOC);
	  
	  if ($result_array['start_datetime']!=null && $result_array['end_datetime']!=null )
	  {
		  $status_array['status']='completed';
	  }
	  elseif ($result_array['start_datetime']!=null && $result_array['end_datetime']==null )
	  {
		  $status_array['status']='awaiting in queue';
	  }
	  else if ($result_array['job_id']==false  )
	  {
		  $status_array['status']='this job does not exist into the Database';
	  }
	  
      echo json_encode(  $status_array  );
      exit();
    }
	elseif (isset($_GET['processor_id']))
    {
      
      $sql = $dbConn->prepare("SELECT count(job_id) FROM priority_queue where processor_id=:processor_id and end_datetime is null");
	  $sql->bindValue(':processor_id', $_GET['processor_id']);
	  $sql->execute();
	  header("HTTP/1.1 200 OK");
	  $contador= $sql->fetch(PDO::FETCH_ASSOC)  ;	  
	  
		  if ($contador['count(job_id)']==0)
		  {
			  $sql2 = $dbConn->prepare("SELECT MIN(job_id) FROM priority_queue where processor_id is null and end_datetime is null");		  
			  $sql2->execute();
			  
			  $job_assigned= $sql2->fetch(PDO::FETCH_ASSOC);
			  $job_assigned_array['job_assigned'] = $job_assigned['MIN(job_id)'];
			  $job_assigned_value=$job_assigned['MIN(job_id)'];
			  $processor_id_value = $_GET['processor_id'];
			  
			  $sql3 = "UPDATE priority_queue SET processor_id='$processor_id_value' WHERE job_id='$job_assigned_value'";
			  $statement = $dbConn->prepare($sql3);
			  $statement->execute();
			  header("HTTP/1.1 200 OK");
			  echo json_encode(  $job_assigned_array  );
			  exit();
		  }
		  else
		  {
			  $job_assigned_array['job_assigned']='There are not jobs in the queue or this processor_id already has a job assigned';
			  echo json_encode(  $job_assigned_array );
			  exit();
		  }
    }
	elseif (isset($_GET['current_average']))		
	{
	  $sql = $dbConn->prepare("SELECT AVG(TIMESTAMPDIFF(MINUTE,start_datetime,end_datetime)) as processing_average_minutes FROM priority_queue ");      
      $sql->execute();
      header("HTTP/1.1 200 OK");
      echo json_encode(  $sql->fetch(PDO::FETCH_ASSOC)  );
      exit();
		
	}
	else
	{
		header("HTTP/1.1 400 Bad Request");
		$message_array['message_error']='Bad request';
		echo json_encode(  $message_array  );
		exit();
	}
   
  
}

//ADD or Insert
if ($_SERVER['REQUEST_METHOD'] == 'POST')
{
    $input = $_POST;
    $sql = "INSERT INTO priority_queue
          (submitter_id, command_execute)
          VALUES
          (:submitter_id,:command_execute)";
    $statement = $dbConn->prepare($sql);
	
    bindAllValues($statement, $input);	
    $statement->execute();
    $postId = $dbConn->lastInsertId();
    if($postId)
    {
      $input['job_id'] = $postId;
      header("HTTP/1.1 200 OK");
      echo json_encode($input);
      exit();
   }
   else
   {
	   header("HTTP/1.1 400 Bad Request");
		$message_array['message_error']='there was an error when the record was to be inserted ';
		echo json_encode(  $message_array  );
		exit();
   }
}

//Delete
/*if ($_SERVER['REQUEST_METHOD'] == 'DELETE')
{
  $id = $_GET['id'];
  $statement = $dbConn->prepare("DELETE FROM priority_queue where job_id=:id");
  $statement->bindValue(':id', $id);
  $statement->execute();
  header("HTTP/1.1 200 OK");
  exit();
}*/


//Update
if ($_SERVER['REQUEST_METHOD'] == 'PUT')
{
    $input = $_GET;
    $job_id = $input['job_id'];
	$processor_id = $input['processor_id'];	
    $fields = getParams($input);
    $sql = "UPDATE priority_queue SET end_datetime=now()  WHERE job_id='$job_id' and processor_id='$processor_id'  ";
    $statement = $dbConn->prepare($sql);
    bindAllValues($statement, $input);
    $statement->execute();
    header("HTTP/1.1 200 OK");
	if($statement->rowCount())
	{
		$result_array['processed']='Success. Job ended';
		echo json_encode($result_array);
	}
	else
	{
		$result_array['processed']='Error';
		echo json_encode($result_array);
	}
	
    exit();
}
//In case others options
header("HTTP/1.1 400 Bad Request");
?>