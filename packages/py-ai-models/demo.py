import asyncio
from py_ai_models import (
    AIGenerator, 
    AIEmbedder, 
    GenerationRequest, 
    EmbeddingRequest, 
    Message
)

async def main():
    # 1. Initialize Clients
    generator = AIGenerator()
    embedder = AIEmbedder()

    print("--- Testing Generation ---")
    # 2. Test Text Generation
    gen_request = GenerationRequest(
        messages=[
            Message(role="system", content="You are a helpful e-commerce assistant."),
            Message(role="user", content="Write a catchy 1-sentence description for a waterproof running shoe.")
        ]
    )
    
    try:
        # Note: This will fail if no API key is set, but it shows the structure
        response = await generator.generate(gen_request)
        print(f"AI Response: {response.content}")
        print(f"Usage: {response.usage.total_tokens} tokens")
        print(f"Latency: {response.latency_ms:.2f}ms")
    except Exception as e:
        print(f"Generation failed (probably missing API key): {e}")

    print("\n--- Testing Embeddings ---")
    # 3. Test Embedding
    embed_request = EmbeddingRequest(
        texts=["Waterproof running shoe", "Lightweight marathon sneaker"]
    )
    
    try:
        embed_response = await embedder.embed(embed_request)
        print(f"Generated {len(embed_response.embeddings)} embeddings.")
        print(f"Vector size: {len(embed_response.embeddings[0])}")
    except Exception as e:
        print(f"Embedding failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
