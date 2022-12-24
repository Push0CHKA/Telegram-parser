from fastapi import FastAPI, Body
from worker import parsComments
from celery.result import AsyncResult

app = FastAPI()


@app.get("/ping")
async def test():
    """
        Test
    """
    return 'pong'


@app.post("/pars")
async def getParsComments(data = Body()):
    """
        Method for parsing comments
    """
    for_pars_list = []
    try:
        limit = int(data['limit'])
        asc = bool(data['asc'])
        data = data['data']
        for dt in data:
            group = dt['url'].replace('https://t.me/', '').split('/')
            source_id = dt['source_id']
            if len(group) == 1:
                group_name = group[0]
                is_post_id = False
                post_id = None
            else:
                group_name = group[0]
                is_post_id = True
                post_id = int(group[1])
            li = [group_name, source_id, is_post_id, post_id, 'https://t.me/' + group_name, limit, asc]
            for_pars_list.append(li)
    except:
        return {'error': 'params error'}
    task = parsComments.delay(for_pars_list)
    return {"task_id": task.id,
            "source_id": source_id}


@app.get("/pars/result")
async def resultPars(task_id: str):
    """
        Metghd for get task 
    """
    task = AsyncResult(task_id)
    return {"task_id": task.id,
            "task_status": task.state,
            "result": task.result}
