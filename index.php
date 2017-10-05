<?php
    set_time_limit(3600);
    $start = microtime(true);
    $servername = "localhost";
    $db_user_name = "saascredit_user";
    $db_password = "wfd,5y/+eB8ocQz";

$conn = pg_connect("host=$servername port=5432 dbname=saascredit_db user=$db_user_name password=$db_password options='--client_encoding=UTF8'")   or die('Could not connect: ' . pg_last_error());
// $cat = Array( );
 pg_set_client_encoding($conn, "UNICODE");
//вход
$f = fopen("dengiclick_1.csv", "rt") or die("Ошибка!");
//виход
$r=fopen("outpars.csv","a+"); 
fputcsv($r, array('Заявка', 'Пролонгаций', 'Старый статус', 'Новый статус', 'Дней просрочки'));
for ($i=0; $data=fgetcsv($f,100,";"); $i++) {
    $qid = $data[0];
    //$qid = 201708230665;
  
$countdaysprosr = pg_query("select coalesce(sum(count),0) as sum from(
            SELECT tab.account_id,
                count(DISTINCT tab.change_time::date) AS count,
                min(tab.change_time)::date AS min,
                max(tab.change_time)::date AS max
               FROM (SELECT account_debt.account_id,
                        account_debt.change_time,
                        account_debt.change_time::date - dense_rank() OVER (ORDER BY account_debt.change_time::date)::integer AS g
                       FROM account_debt
                      join questionnaires q on q.account_id = account_debt.account_id and number  = '$qid' 
                      WHERE account_debt.penalties_debt > 0::numeric and account_debt.account_id = q.account_id and account_debt.change_time::date <= now()
                      GROUP BY account_debt.account_id, account_debt.change_time
                      ORDER BY account_debt.change_time) tab
              GROUP BY tab.account_id, tab.g)res") or die('Ошибка запроса: ' . pg_last_error());
$countdaysprosr_q = pg_fetch_array($countdaysprosr, 0, PGSQL_NUM);

$newstat = pg_query("select dic_questionnaires_status_type.name, questionnaires.id
from
questionnaires

join
status_questionnaires on status_questionnaires.questionnaires_id = questionnaires.id -- and (status_questionnaires.status_time = max(status_questionnaires.status_time))
join
dic_questionnaires_status_type on dic_questionnaires_status_type.id = status_questionnaires.status_type_id

where
questionnaires.number = '$qid'
ORDER BY
status_time DESC
LIMIT 1") or die('Ошибка запроса: ' . pg_last_error());
$newstat_q = pg_fetch_array($newstat, 0, PGSQL_NUM);
    
$countprolong = pg_query("select COUNT(*) as total from questionnaire_extentions where questionnaire_id = ".$newstat_q[1]) or die('Ошибка запроса: ' . pg_last_error());    
$countprolong_q = pg_fetch_array($countprolong, 0, PGSQL_NUM); 
    
$out = array(
    $qid,//номер заявки
    $countprolong_q[0],//количество пролонгаций
    $data[1],//старый статус
    $newstat_q[0],//новый статус
    $countdaysprosr_q[0]//дней просрочки
);
    //var_dump($out);
fputcsv($r, $out);
  
}
fclose($f); 
fclose($r);
$time = microtime(true) - $start;
printf('Скрипт выполнялся %.4F сек.', $time);
        

/*
select
questionnaires.number, -- Выводим номер заявки
status_questionnaires.status_type_id, -- Выводим id статуса
dic_questionnaires_status_type.name, -- Выводим имя статуса
status_questionnaires.status_time AS status_time, -- выводим время установки статуса
status_questionnaires.accept,
questionnaire_extentions.count_penalties -- Выводим кол-во штрафных дней
from
questionnaires

join
status_questionnaires on status_questionnaires.questionnaires_id = questionnaires.id -- and (status_questionnaires.status_time = max(status_questionnaires.status_time))
join
dic_questionnaires_status_type on dic_questionnaires_status_type.id = status_questionnaires.status_type_id
left join
-- questionnaire_extentions on questionnaire_extentions.questionnaire_id = questionnaires.id
(select coalesce(sum(count),0) as sum from(
            SELECT tab.account_id,
                count(DISTINCT tab.change_time::date) AS count,
                min(tab.change_time)::date AS min,
                max(tab.change_time)::date AS max
               FROM (SELECT account_debt.account_id,
                        account_debt.change_time,
                        account_debt.change_time::date - dense_rank() OVER (ORDER BY account_debt.change_time::date)::integer AS g
                       FROM account_debt
                      join questionnaires q on q.account_id = account_debt.account_id and number  = '201708230665' 
                      WHERE account_debt.penalties_debt > 0::numeric and account_debt.account_id = q.account_id and account_debt.change_time::date <= now()
                      GROUP BY account_debt.account_id, account_debt.change_time
                      ORDER BY account_debt.change_time) tab
              GROUP BY tab.account_id, tab.g)res) res

where
questionnaires.number = '201708230665'
ORDER BY
status_time DESC
LIMIT 1

select
-- questionnaires.number, -- Выводим номер заявки
-- status_questionnaires.status_type_id, -- Выводим id статуса
dic_questionnaires_status_type.name -- Выводим имя статуса
-- status_questionnaires.status_time AS status_time -- выводим время установки статуса

from
questionnaires

join
status_questionnaires on status_questionnaires.questionnaires_id = questionnaires.id -- and (status_questionnaires.status_time = max(status_questionnaires.status_time))
join
dic_questionnaires_status_type on dic_questionnaires_status_type.id = status_questionnaires.status_type_id

where
questionnaires.number = '201708230665'
ORDER BY
status_time DESC
LIMIT 1

*/