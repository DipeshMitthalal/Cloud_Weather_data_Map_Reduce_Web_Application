<html>
<head><title>Weather Information!</title></head>
    <body>
<div style="border-width: 1px;
                  border-style: solid;
                  border-color: #ff0000;
                  padding: 15px;"> 
        <?php
 
ini_set('max_execution_time', 300);
//echo "Welcome from the act.php.</br>";
//echo "The Current time is ".time()."</br>";
//echo "Waiting for 100 seconds</br>";
$wait_Time=$_GET["wait_Time"];
$var=time();
while(time() - $var <$wait_Time)
;
//echo "compled waiting for 100 seconds.</br>";
//echo "The current time is ".time()."</br>";
$diff=time()-$var;
echo "waited for the time ".$diff." seconds</br>";
   



                require_once dirname(__FILE__) . '/sdk/sdk.class.php';
        $dynamodb1 = new AmazonDynamoDB();
        
        //$dynamodb1->set_region("REGION_EU_W1");       
       
        $City = $_GET["City"];
        $Date = $_GET["Date"];
        $Subject = $_GET["subject"];
//echo $City;
//echo $Date;
//echo $Subject;
        $response1 = $dynamodb1->query(array(
            'TableName' => 'weatherdata',
            'HashKeyValue' => array(AmazonDynamoDB::TYPE_STRING =>$City),
            'AttributesToGet' => array($Subject),
//  'ConsistentRead' => true,
             'RangeKeyCondition' => array(
        'ComparisonOperator' => AmazonDynamoDB::CONDITION_EQUAL,
        'AttributeValueList' => array(
            array( AmazonDynamoDB::TYPE_NUMBER => $Date )
                )
            )
			));
		
//		echo 'I reahced here';
if (empty($response1->body->Items->$Subject)) {
    echo 'Data is not available';
}else
{
//echo 'Data is  available';
 echo "<br><strong>$Subject: </strong>"
         . (string)$response1->body->Items->$Subject->{AmazonDynamoDB::TYPE_NUMBER} ."</p>";

}
?>
    </body>
</html>

