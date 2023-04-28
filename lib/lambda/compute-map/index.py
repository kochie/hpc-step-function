import math


def multinomial(*vals):
    result = 1
    for i in vals:
        result *= math.factorial(i)
    return math.factorial(sum(vals)) // result


def handler(event, context):
    result = 0

    i = int(event["index"])
    for j in range(0, event["size"]//2+1):
        if 2*j > i:
            break

        # @show i, j
        for k in range(0, event["size"]//3+1):
            if 2*j + 3*k > i:
                break

            for l in range(0, event["size"]//4+1):
                if 2*j + 3*k + 4*l > i:
                    break

                result += multinomial(i - 2*j - 3*k - 4*l, j, k, l) * 128 ** (
                    i - 2*j - 3*k - 4*l) * 1863 ** (j) * 42451 ** (k) * 78341 ** (l)

    event.update({"result": result})
    return event
